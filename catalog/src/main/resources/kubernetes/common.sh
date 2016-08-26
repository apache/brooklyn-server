# Common

if [ ! -d "/opt/kubernetes/bin" ] ; then
  # Configure /etc/hosts
  echo "${MASTER_IP} 4-kube-master" | sudo tee --append /etc/hosts > /dev/null
  echo "${NODE_IP} 4-kube-node-1" | sudo tee --append /etc/hosts > /dev/null

  # Install wget
  sudo yum -y install wget

  # Download
  DOWNLOAD=/tmp/download
  mkdir -p ${DOWNLOAD}
  wget "https://github.com/coreos/flannel/releases/download/v${FLANNEL_VERSION}/flannel-v${FLANNEL_VERSION}-linux-amd64.tar.gz" -O ${DOWNLOAD}/flannel.tar
  wget "https://github.com/coreos/etcd/releases/download/v${ETCD_VERSION}/etcd-v${ETCD_VERSION}-linux-amd64.tar.gz" -O ${DOWNLOAD}/etcd.tar.gz
  wget "https://get.docker.com/builds/Linux/x86_64/docker-${DOCKER_VERSION}.tgz" -O ${DOWNLOAD}/docker.tar.gz

  # Install binaries
  sudo mkdir -p /opt/kubernetes/bin
  sudo mkdir -p /opt/kubernetes/cfg

  tar xf ${DOWNLOAD}/flannel.tar -C ${DOWNLOAD}
  sudo cp ${DOWNLOAD}/flanneld /opt/kubernetes/bin

  tar xzf ${DOWNLOAD}/etcd.tar.gz -C ${DOWNLOAD}
  sudo cp ${DOWNLOAD}/etcd-v${ETCD_VERSION}-linux-amd64/etcd ${DOWNLOAD}/etcd-v${ETCD_VERSION}-linux-amd64/etcdctl /opt/kubernetes/bin

  sudo tar --strip-components=1 -xvzf ${DOWNLOAD}/docker.tar.gz -C /opt/kubernetes/bin

  for k8s_file in kube-apiserver kube-controller-manager kube-scheduler kube-proxy kubelet kubectl ; do
    sudo wget -N -P /opt/kubernetes/bin https://storage.googleapis.com/kubernetes-release/release/v${K8S_VERSION}/bin/linux/amd64/${k8s_file}
    sudo chmod +x /opt/kubernetes/bin/${k8s_file}
  done

  for file in /opt/kubernetes/bin/* ; do sudo ln -s ${file} /usr/bin/$(basename ${file}) ; done
fi
