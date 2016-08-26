# Master

# cd k8s-spike/ && git checkout origin/userspace && source ./env.sh
# ./master.sh

# set -x
ROOT=$(cd $(dirname $0) ; pwd)

# Setup
[ -z "${MASTER_IP}" ] && . ${ROOT}/env.sh
. ${ROOT}/common.sh

# Start etcd
sudo mkdir -p /var/lib/etcd
sudo -E tee /opt/kubernetes/cfg/etcd.conf <<-EOF
ETCD_NAME="k8s"
ETCD_DATA_DIR="/var/lib/etcd/k8s.etcd"
ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:4001"
ETCD_ADVERTISE_CLIENT_URLS="${ETCD_SERVERS}"
EOF

sudo -E tee //usr/lib/systemd/system/etcd.service <<-EOF
[Unit]
Description=Etcd Server
After=network.target
[Service]
Type=simple
WorkingDirectory=/var/lib/etcd
EnvironmentFile=-/opt/kubernetes/cfg/etcd.conf
ExecStart=/bin/bash -c "GOMAXPROCS=1 /opt/kubernetes/bin/etcd"
[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable etcd
sudo systemctl start etcd

# Run Flannel, Docker and Calico (as per Node)
export NODE_IP="${MASTER_IP}"
${ROOT}/node.sh
. /run/flannel/subnet.env
export FLANNEL_NETWORK FLANNEL_SUBNET

# Start API Server
sudo -E tee /usr/lib/systemd/system/kube-apiserver.service <<-EOF
[Unit]
Description=Kubernetes API Server
Documentation=https://github.com/kubernetes/kubernetes
[Service]
ExecStart=/opt/kubernetes/bin/kube-apiserver \
  --logtostderr=true \
  --v=4 \
  --etcd-servers=${ETCD_SERVERS} \
  --insecure-bind-address=0.0.0.0 \
  --insecure-port=8080 \
  --advertise-address=${MASTER_IP} \
  --allow-privileged=true \
  --service-cluster-ip-range=${SERVICE_CLUSTER_IP_RANGE} \
  --runtime-config=extensions/v1beta1=true,extensions/v1beta1/networkpolicies=true \
  --admission-control=${ADMISSION_CONTROL}
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable kube-apiserver
sudo systemctl start kube-apiserver

# Start Controller Manager
sudo -E tee /usr/lib/systemd/system/kube-controller-manager.service <<-EOF
[Unit]
Description=Kubernetes Controller Manager
Documentation=https://github.com/kubernetes/kubernetes
[Service]
ExecStart=/opt/kubernetes/bin/kube-controller-manager \
  --logtostderr=true \
  --v=4 \
  --master=${MASTER_IP}:8080 \
  --allocate-node-cidrs=true \
  --leader-elect=true \
  --cluster-cidr=${FLANNEL_NETWORK}
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable kube-controller-manager
sudo systemctl start kube-controller-manager

# Start Scheduler
sudo -E tee /usr/lib/systemd/system/kube-scheduler.service <<-EOF
[Unit]
Description=Kubernetes Scheduler
Documentation=https://github.com/kubernetes/kubernetes
[Service]
ExecStart=/opt/kubernetes/bin/kube-scheduler \
  --logtostderr=true \
  --v=4 \
  --leader-elect=true \
  --master=${MASTER_IP}:8080
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable kube-scheduler
sudo systemctl start kube-scheduler

# Create the Calico system namespace
kubectl create ns calico-system

# Start the Calico policy controller pod
sudo -E tee /etc/kubernetes/manifests/policy-controller.yaml <<-EOF
apiVersion: v1
kind: Pod
metadata:
  name: policy-controller
  namespace: calico-system
  labels:
    version: "latest"
    projectcalico.org/app: "policy-controller"
spec:
  hostNetwork: true
  containers:
    # The Calico policy controller.
    - name: policy-controller
      image: calico/kube-policy-controller:latest
      env:
        - name: ETCD_ENDPOINTS
          value: "http://${MASTER_IP}:4001"
        - name: K8S_API
          value: "http://${MASTER_IP}:8080"
        - name: LEADER_ELECTION
          value: "true"
    # Leader election container used by the policy controller.
    - name: leader-elector
      image: quay.io/calico/leader-elector:v0.1.0
      imagePullPolicy: IfNotPresent
      args:
        - "--election=calico-policy-election"
        - "--election-namespace=calico-system"
        - "--http=127.0.0.1:4040"
EOF

cat > dns-addon.yaml <<-EOF
apiVersion: v1
kind: Service
metadata:
  name: kube-dns
  namespace: kube-system
  labels:
    k8s-app: kube-dns
    kubernetes.io/cluster-service: "true"
    kubernetes.io/name: "KubeDNS"
spec:
  selector:
    k8s-app: kube-dns
  clusterIP: ${DNS_SERVICE_IP}
  ports:
  - name: dns
    port: 53
    protocol: UDP
  - name: dns-tcp
    port: 53
    protocol: TCP

---

apiVersion: v1
kind: ReplicationController
metadata:
  name: kube-dns-v17.1
  namespace: kube-system
  labels:
    k8s-app: kube-dns
    version: v17.1
    kubernetes.io/cluster-service: "true"
spec:
  replicas: 1
  selector:
    k8s-app: kube-dns
    version: v17.1
  template:
    metadata:
      labels:
        k8s-app: kube-dns
        version: v17.1
        kubernetes.io/cluster-service: "true"
    spec:
      containers:
      - name: kubedns
        image: gcr.io/google_containers/kubedns-amd64:1.5
        resources:
          limits:
            cpu: 100m
            memory: 170Mi
          requests:
            cpu: 100m
            memory: 70Mi
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
            scheme: HTTP
          initialDelaySeconds: 60
          timeoutSeconds: 5
          successThreshold: 1
          failureThreshold: 5
        readinessProbe:
          httpGet:
            path: /readiness
            port: 8081
            scheme: HTTP
          # we poll on pod startup for the Kubernetes master service and
          # only setup the /readiness HTTP server once that's available.
          initialDelaySeconds: 30
          timeoutSeconds: 5
        args:
        # command = "/kube-dns"
        - --domain=cluster.local.
        - --dns-port=10053
        ports:
        - containerPort: 10053
          name: dns-local
          protocol: UDP
        - containerPort: 10053
          name: dns-tcp-local
          protocol: TCP
      - name: dnsmasq
        image: gcr.io/google_containers/kube-dnsmasq-amd64:1.3
        args:
        - --cache-size=1000
        - --no-resolv
        - --server=127.0.0.1#10053
        ports:
        - containerPort: 53
          name: dns
          protocol: UDP
        - containerPort: 53
          name: dns-tcp
          protocol: TCP
      - name: healthz
        image: gcr.io/google_containers/exechealthz-amd64:1.1
        resources:
          # keep request = limit to keep this container in guaranteed class
          limits:
            cpu: 10m
            memory: 50Mi
          requests:
            cpu: 10m
            memory: 50Mi
        args:
        - -cmd=nslookup kubernetes.default.svc.cluster.local 127.0.0.1 >/dev/null && nslookup kubernetes.default.svc.cluster.local 127.0.0.1:10053 >/dev/null
        - -port=8080
        - -quiet
        ports:
        - containerPort: 8080
          protocol: TCP
      dnsPolicy: Default  # Don't use cluster DNS.
EOF

kubectl create -f ./dns-addon.yml
sleep 10
kubectl get pods --namespace=kube-system | grep kube-dns-v17.1

cat > kube-dashboard-rc.json <<-EOF
{
  "apiVersion": "v1",
  "kind": "ReplicationController",
  "metadata": {
    "labels": {
      "k8s-app": "kubernetes-dashboard",
      "kubernetes.io/cluster-service": "true",
      "version": "v1.1.0"
    },
    "name": "kubernetes-dashboard-v1.1.0",
    "namespace": "kube-system"
  },
  "spec": {
    "replicas": 1,
    "selector": {
      "k8s-app": "kubernetes-dashboard"
    },
    "template": {
      "metadata": {
        "labels": {
          "k8s-app": "kubernetes-dashboard",
          "kubernetes.io/cluster-service": "true",
          "version": "v1.1.0"
        }
      },
      "spec": {
        "containers": [
          {
            "image": "gcr.io/google_containers/kubernetes-dashboard-amd64:v1.1.0",
            "livenessProbe": {
              "httpGet": {
                "path": "/",
                "port": 9090
              },
              "initialDelaySeconds": 30,
              "timeoutSeconds": 30
            },
            "name": "kubernetes-dashboard",
            "ports": [
              {
                "containerPort": 9090
              }
            ],
            "resources": {
              "limits": {
                "cpu": "100m",
                "memory": "50Mi"
              },
              "requests": {
                "cpu": "100m",
                "memory": "50Mi"
              }
            }
          }
        ]
      }
    }
  }
}
EOF

cat > kube-dashboard-svc.json <<-EOF
{
  "apiVersion": "v1",
  "kind": "Service",
  "metadata": {
    "labels": {
      "k8s-app": "kubernetes-dashboard",
      "kubernetes.io/cluster-service": "true"
    },
    "name": "kubernetes-dashboard",
    "namespace": "kube-system"
  },
  "spec": {
    "ports": [
      {
        "port": 80,
        "targetPort": 9090
      }
    ],
    "selector": {
      "k8s-app": "kubernetes-dashboard"
    }
  }
}
EOF

kubectl create -f kube-dashboard-rc.json
kubectl create -f kube-dashboard-svc.json
sleep 10
kubectl get pods --namespace=kube-system
dashboard_pod_id=$(kubectl get pods --namespace=kube-system | grep kubernetes-dashboard-v1.1.0 | awk '{ print $1; }')
kubectl port-forward ${dashboard_pod_id} 9090 --namespace=kube-system

for service in etcd kube-apiserver kube-controller-manager kube-scheduler ; do
    sudo systemctl status ${service}
done
