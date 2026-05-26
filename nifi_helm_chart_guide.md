# Install
`git clone --recursive https://github.com/hotcat190/sensor-data-pipeline`

or
```
git pull hotcat190
git submodule update --recursive --remote
```
then install helm dependencies

```
helm repo add jetstack https://charts.jetstack.io
helm repo update

helm install cert-manager jetstack/cert-manager \`
  --namespace cert-manager \`
  --create-namespace \`
  --set installCRDs=true

helm dependency update apache-nifi-helm
```
install chart:
- 1 node, basic auth
```
helm install my-nifi apache-nifi-helm -f apache-nifi-helm/examples/values-basic-auth.yaml -n nifi --create-namespace
```
- 3 node, oidc (refer to setup_oidc.md)
```
helm install my-nifi apache-nifi-helm -f apache-nifi-helm/values.yaml -n nifi --create-namespace
```

# After pods are running
Open your system's hosts file (/etc/hosts or C:\Windows\System32\drivers\etc\hosts) and add the following entry:
`127.0.0.1 example.com`
port-forward
```
kubectl port-forward pod/my-nifi-0 8443:8443 -n nifi
```
# Open https://example.com:8443/nifi
# Default credentials: admin@company.com / Password1234

# Troubleshooting
## PersistentVolumeClaims
"0/1 nodes are available: pod has unbound immediate PersistentVolumeClaims. preemption: 0/1 nodes are available: 1 Preemption is not helpful for scheduling."

```
helm uninstall my-nifi -n nifi
kubectl delete pvc -l app.kubernetes.io/instance=my-nifi -n nifi
```
Change all `storageClass` in `values.yaml` and `examles/values*.yaml` to empty string `""`
```
helm install my-nifi apache-nifi-helm -f apache-nifi-helm/examples/values-basic-auth.yaml -n nifi --create-namespace
```

## Zookeeper pods appearing and failing
set
```
zookeeper:
  enabled: false
```
in `apache-nifi-helm/values.yaml`
NiFi 2.0+ supports Kubernetes Native

## Auth
```
kubectl logs my-nifi-0 -n nifi | findstr auth
kubectl get secrets -n nifi | findstr nifi
kubectl exec -n nifi -it my-nifi-0 -- curl -k http://keycloak.nifi.svc.cluster.local:80/.well-known/openid-configuration
```

## Login errors
```diff
diff --git a/cert.yaml b/cert.yaml
--- a/cert.yaml
+++ b/cert.yaml
@@ -18,4 +18,6 @@
     {{- include "nifi.hostNodeList" . | nindent 4 }}
     {{- include "nifi.ingressNodeList" . | nindent 4 }}
+  ipAddresses:
+    - 0.0.0.0
   usages:
     - server auth
```
```
kubectl delete secret my-nifi-tls -n nifi
```

# Monitoring
```
kubectl get pods -n nifi -w
kubectl describe pods my-nifi-0 -n nifi
kubectl describe pods my-nifi-1 -n nifi
kubectl describe pods my-nifi-2 -n nifi
```

# Upgrading (after changing values)
```
helm upgrade my-nifi apache-nifi-helm -f apache-nifi-helm/values.yaml -n nifi --create-namespace
```

# Rollout restart
```
kubectl rollout restart statefulset my-nifi -n nifi
```

# Uninstall
```
helm uninstall my-nifi -n nifi
```