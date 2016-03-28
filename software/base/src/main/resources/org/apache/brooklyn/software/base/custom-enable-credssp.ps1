#!ps1
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Resources:
#  https://github.com/mwrock/boxstarter/blob/master/LICENSE.txt
#  https://github.com/mwrock/boxstarter/blob/master/Boxstarter.Chocolatey/Enable-BoxstarterCredSSP.ps1

function Custom-Enable-CredSSP {
<#
.SYNOPSIS
Enables and configures CredSSP Authentication to be used in PowerShell remoting sessions

.DESCRIPTION
Enabling CredSSP allows a caller from one remote session to authenticate on other remote
resources. This is known as credential delegation. By default, PowerShell sessions do not
use credSSP and therefore cannot bake a "second hop" to use other remote resources that
require their authentication token.

This command will enable CredSSP and add all RemoteHostsToTrust to the CredSSP trusted
hosts list. It will also edit the users group policy to allow Fresh Credential Delegation.

.PARAMETER RemoteHostsToTrust
A list of ComputerNames to add to the CredSSP Trusted hosts list.

.OUTPUTS
A list of the original trusted hosts on the local machine.

.EXAMPLE
Custom-Enable-CredSSP box1,box2


#>
    param(
    [string[]] $RemoteHostsToTrust
    )

    # Required to be running for using CredSSP
    winrm quickconfig -transport:http -quiet

    & winrm set winrm/config/service/auth '@{CredSSP="true"}'
    If ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    & winrm set winrm/config/client/auth '@{CredSSP="true"}'
    If ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    $Result=@{
        Success=$False;
        PreviousCSSPTrustedHosts=$null;
        PreviousFreshCredDelegationHostCount=0
    }

    Write-Host "Configuring CredSSP settings..."
    $credssp = Get-WSManCredSSP

    $ComputersToAdd = @()
    $idxHosts=$credssp[0].IndexOf(": ")
    if($idxHosts -gt -1){
        $credsspEnabled=$True
        $Result.PreviousCSSPTrustedHosts=$credssp[0].substring($idxHosts+2)
        $hostArray=$Result.PreviousCSSPTrustedHosts.Split(",")
        $RemoteHostsToTrust | ? { $hostArray -notcontains "wsman/$_" } | % { $ComputersToAdd += $_ }
    }
    else {
        $ComputersToAdd = $RemoteHostsToTrust
    }

    if($ComputersToAdd.Count -gt 0){
        try {
            Enable-WSManCredSSP -DelegateComputer $ComputersToAdd -Role Client -Force -ErrorAction Stop | Out-Null
        }
        catch {
            Write-Error "Enable-WSManCredSSP failed with: $_" -Verbose
            return $result
        }
    }

    $key = "HKLM:\SOFTWARE\Policies\Microsoft\Windows"
    if (!(Test-Path "$key\CredentialsDelegation")) {
        New-Item $key -Name CredentialsDelegation | Out-Null
    }
    $key = Join-Path $key "CredentialsDelegation"
    New-ItemProperty -Path "$key" -Name "ConcatenateDefaults_AllowFresh" -Value 1 -PropertyType Dword -Force | Out-Null
    New-ItemProperty -Path "$key" -Name "ConcatenateDefaults_AllowFreshNTLMOnly" -Value 1 -PropertyType Dword -Force | Out-Null

    $result.PreviousFreshNTLMCredDelegationHostCount = Set-CredentialDelegation $key 'AllowFreshCredentialsWhenNTLMOnly' $RemoteHostsToTrust
    $result.PreviousFreshCredDelegationHostCount = Set-CredentialDelegation $key 'AllowFreshCredentials' $RemoteHostsToTrust

    $Result.Success=$True
    return $Result
}

function Set-CredentialDelegation($key, $subKey, $allowed){
    New-ItemProperty -Path "$key" -Name $subKey -Value 1 -PropertyType Dword -Force | Out-Null
    $policyNode = Join-Path $key $subKey
    if (!(Test-Path $policyNode)) {
        md $policyNode | Out-Null
    }
    $currentHostProps=@()
    (Get-Item $policyNode).Property | % {
        $currentHostProps += (Get-ItemProperty -Path $policyNode -Name $_).($_)
    }
    $currentLength = $currentHostProps.Length
    $idx=$currentLength
    $allowed | ? { $currentHostProps -notcontains "wsman/$_"} | % {
        ++$idx
        New-ItemProperty -Path $policyNode -Name "$idx" -Value "wsman/$_" -PropertyType String -Force | Out-Null
    }

    return $currentLength
}

$result = Custom-Enable-CredSSP $env:COMPUTERNAME,localhost
if (-not $result.Success) {
  exit 1
}