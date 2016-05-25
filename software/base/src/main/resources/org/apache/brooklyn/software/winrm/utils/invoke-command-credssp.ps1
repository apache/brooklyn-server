[#ftl]
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
Param([Parameter(Mandatory=$True)][String]$Command, [String[]] $ArgumentList, [switch] $LogOutputInFile, $ScriptBlockInCredSSP)
<#
.SYNOPSIS
Helper Script which executes commands through Invoke-Command -Authentication CredSSP

.DESCRIPTION
By default, PowerShell sessions do not
use credSSP and therefore cannot bake a "second hop" to use other remote resources that
require their authentication token.
Microsoft: "CAUTION: Credential Security Support Provider (CredSSP) authentication, in which the user's credentials are passed to a remote computer to be authenticated, is designed for commands that require authentication on more than one resource, such as accessing a remote network share. This mechanism increases the security risk of the remote operation."
Passing parameters in powershell: https://technet.microsoft.com/en-us/magazine/jj554301.aspx

.NOTES
The script may not work properly on a machine which has installed Active Directory.

.PARAMETER Command
The command which you want to invoke through CredSSP

.PARAMETER ArgumentList
A list of arguments you want to pass to $Command which will be executed.

.PARAMETER LogOutputInFile
Redirect output to a file in $env:TEMP folder

.OUTPUTS
The output of the command.

.EXAMPLE

If you are inside command prompt and you want to run a native command or a batch script with CredSSP:

powershell -command C:\invoke-command-credssp.ps1 -Command C:\setup.exe "/q","/param2"

.EXAMPLE

If you are inside powershell and you want to run a native command or batch script with CredSSP:

C:\Invoke-Command-Credssp.ps1 -Com C:\test.bat -ArgumentList ("/q",/"f")

.EXAMPLE

If you are in command prompt and want to run another powershell script with CredSSP:

powershell -command "C:\Invoke-Command-Credssp -Command powershell -ArgumentList (\"-command\",\"c:\hi_params.ps1\")"

.EXAMPLE

If you are inside powershell and want to execute a powershell block through CredSSP.
Then you can use the special -ScriptBlock parameter.
Since -Command is threated as the most common case and it is made mandatory parameter,
Just pass an empty command and a Script-Block which you want to be executed through CredSSP

C:\Invoke-Command-Credssp -Command "empty" -ScriptBlock {Write-Host "A script block which is using CredSSP..."; 0}

#>

$exitCode = 1
Try {
  $pass = '${attribute['windows.password']}'
  $secpasswd = ConvertTo-SecureString $pass -AsPlainText -Force
  $mycreds = New-Object System.Management.Automation.PSCredential ($($env:COMPUTERNAME + "\${location.user}"), $secpasswd)

  $exitCode = Invoke-Command -Credential $mycreds -ComputerName $env:COMPUTERNAME -ScriptBlock {
    param($Command,$ArgumentList,$LogOutputInFile,$ScriptBlockInCredSSP)
    $startProcArgs = If ($ArgumentList.Length -gt 0) { @{ArgumentList = $ArgumentList} } Else { @{} }
    If ($ScriptBlockInCredSSP) {
      $ScriptBlockInCredSSP = ([scriptblock]::Create($ScriptBlockInCredSSP))
      $r = & $ScriptBlockInCredSSP
      if ($r -is [int]) {
        Write-Host "ScriptBlock reported that its status is ($r)"
        $r
      } else {
        Write-Host "ScriptBlock didn't report its status"
        0
      }
    } ElseIf ($LogOutputInFile) {
      $stdFilePathNoExt = "$($Env:TEMP)\$(Split-Path $Command -Leaf)_$((Get-Date).Ticks / 1000)"
      $stdOutFile = "$($stdFilePathNoExt).stdout.log"
      $stdErrFile = "$($stdFilePathNoExt).stderr.log"
      $process = Start-Process $Command @startProcArgs -RedirectStandardOutput $stdOutFile -RedirectStandardError $stdErrFile -Wait -PassThru -NoNewWindow
      $process.ExitCode
    } Else {
      $process = Start-Process $Command @startProcArgs -Wait -PassThru -NoNewWindow
      $process.ExitCode
    }
  } -Authentication CredSSP -ArgumentList $Command,$ArgumentList,$LogOutputInFile.IsPresent,$ScriptBlockInCredSSP
} Catch {
  Write-Error $_.Exception
  Write-Host 'Exception logged'
  exit 1
}
exit $exitCode
