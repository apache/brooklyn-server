# Node

# cd k8s-spike/ && git checkout origin/userspace && source ./env.sh
# ./node.sh

# set -x
ROOT=$(cd $(dirname $0) ; pwd)

# Setup
[ -z "${MASTER_IP}" ] && . ${ROOT}/env.sh
. ${ROOT}/common.sh

# Run Node services

# Start Flannel
sudo ip link set dev docker0 down
sudo ip link delete docker0
sudo iptables -F -t nat

sudo -E tee /usr/lib/systemd/system/flannel.service <<-EOF
[Unit]
Description=Flanneld overlay address etcd agent
After=network.target
Before=docker.service
[Service]
ExecStart=/opt/kubernetes/bin/flanneld \
  --ip-masq \
  --public-ip=${NODE_IP} \
  -etcd-endpoints=${ETCD_SERVERS} \
  -etcd-prefix=/coreos.com/network
Type=notify
[Install]
WantedBy=multi-user.target
RequiredBy=docker.service
EOF

# Store FLANNEL_NET to etcd.
while true; do
  /opt/kubernetes/bin/etcdctl --no-sync -C ${ETCD_SERVERS} \
    get /coreos.com/network/config >/dev/null 2>&1
  if [[ "$?" == 0 ]]; then
    break
  else
    if ((++attempt > 600 )); then
      echo "timeout for waiting network config" > ~/kube/err.log
      exit 2
    fi
    /opt/kubernetes/bin/etcdctl --no-sync -C ${ETCD_SERVERS} \
      mk /coreos.com/network/config "{\"Network\":\"${FLANNEL_NET}\"}" >/dev/null 2>&1
    sleep 3
  fi
done
wait

sudo systemctl daemon-reload
sudo systemctl enable flannel
sudo systemctl start flannel

while [ ! -f /run/flannel/subnet.env ] ; do sleep 10 ; done
. /run/flannel/subnet.env
export FLANNEL_NETWORK FLANNEL_SUBNET FLANNEL_MTU
sudo iptables -t nat -A POSTROUTING ! -d ${FLANNEL_NETWORK} -o eth0 -j MASQUERADE
sudo yum install -q -y bridge-utils
sudo brctl addbr cbr0
sudo ifconfig cbr0 ${FLANNEL_SUBNET} up

# Start Docker
sudo -E tee /usr/lib/systemd/system/docker.service <<-EOF
[Unit]
Description=Docker Application Container Engine
Documentation=http://docs.docker.com
After=network.target flannel.service
Requires=flannel.service
[Service]
Type=notify
EnvironmentFile=-/run/flannel/subnet.env
WorkingDirectory=/opt/kubernetes/bin
ExecStart=/opt/kubernetes/bin/docker daemon \
  -H tcp://127.0.0.1:4243 \
  -H unix:///var/run/docker.sock \
  --selinux-enabled=false \
  --mtu=${FLANNEL_MTU} \
  --bridge=cbr0 \
  --ip-masq=false \
  --iptables=false
LimitNOFILE=1048576
LimitNPROC=1048576
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable docker
sudo systemctl start docker

# Install and Configure calico/node
sleep 60
sudo wget -N -P /usr/bin https://github.com/projectcalico/calico-containers/releases/download/v${CALICO_VERSION}/calicoctl
sudo chmod +x /usr/bin/calicoctl
sudo modprobe xt_set
sudo modprobe ip6_tables
sudo /home/core/calicoctl checksystem --fix
sudo -E tee /etc/systemd/system/calico-node.service <<-EOF
[Unit]
Description=calicoctl node
After=docker.service
Requires=docker.service
[Service]
User=root
Environment=ETCD_AUTHORITY=${MASTER_IP}:4001
PermissionsStartOnly=true
ExecStart=/usr/bin/calicoctl node --ip=${NODE_IP} --detach=false
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable calico-node
sudo systemctl start calico-node

# Start Proxy
sudo -E tee /usr/lib/systemd/system/kube-proxy.service <<-EOF
[Unit]
Description=Kubernetes Proxy
After=network.target
[Service]
ExecStart=/opt/kubernetes/bin/kube-proxy \
  --logtostderr=true \
  --v=4 \
  --masquerade-all=true \
  --master=http://${MASTER_IP}:8080
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable kube-proxy
sudo systemctl start kube-proxy

# Install CNI
sudo mkdir -p /opt/cni/bin
sudo wget https://github.com/containernetworking/cni/releases/download/v${CNI_VERSION}/cni-v${CNI_VERSION}.tgz -O /tmp/download/cni.tgz
sudo tar --strip-components=1 -xvzf /tmp/download/cni.tgz -C /opt/cni/bin

# Configure Kubelet to use CNI Plugin
sudo mkdir -p /etc/cni/net.d
sudo -E tee /etc/cni/net.d/10-calico.conf <<-EOF
{
    "name": "calico",
    "type": "flannel",
    "delegate": {
        "type": "calico",
        "etcd_authority": "${MASTER_IP}:4001",
        "log_level": "debug",
        "log_level_stderr": "info",
        "hostname": "${NODE_IP}",
        "policy": {
            "type": "k8s",
            "k8s_api_root": "http://${MASTER_IP}:8080/api/v1/"
        }
    },
    "ipam": {
        "type": "calico-ipam"
    }
}
EOF

sudo wget -N -P /opt/cni/bin https://github.com/projectcalico/calico-cni/releases/download/v${CALICO_CNI_VERSION}/calico
sudo chmod +x /opt/cni/bin/calico
sudo wget -N -P /opt/cni/bin https://github.com/projectcalico/calico-cni/releases/download/v${CALICO_CNI_VERSION}/calico-ipam
sudo chmod +x /opt/cni/bin/calico-ipam

if [ "${NODE_IP}" == "${MASTER_IP}" ] ; then
    EXTRA_OPTS="--register-schedulable=false"
fi

# Start Kubelet
sudo mkdir -p /etc/kubernetes/manifests
sudo -E tee /usr/lib/systemd/system/kubelet.service <<-EOF
[Unit]
Description=Kubernetes Kubelet
After=docker.service
Requires=docker.service
[Service]
ExecStart=/usr/bin/kubelet \
  --address=0.0.0.0 \
  --allow-privileged=true \
  --config=/etc/kubernetes/manifests \
  --hostname-override=${NODE_IP} \
  --api-servers=http://${MASTER_IP}:8080 \
  --network-plugin-dir=/etc/cni/net.d \
  --network-plugin=cni \
  ${EXTRA_OPTS} \
  --pod-cidr=${FLANNEL_SUBNET} \
  --container-runtime=docker \
  --reconcile-cidr=true \
  --serialize-image-pulls=false \
  --cluster-dns=${DNS_SERVICE_IP} \
  --cluster-domain=cluster.local
  --logtostderr=true
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target
EOF
#  --configure-cbr0=true \
sudo systemctl daemon-reload
sudo systemctl enable kubelet
sudo systemctl start kubelet

for service in flannel docker calico-node kube-proxy kubelet; do
    sudo systemctl status ${service}
done
