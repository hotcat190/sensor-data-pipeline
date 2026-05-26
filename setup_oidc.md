Setting up OpenID Connect (OIDC) locally using **Keycloak** is the most practical way to test a 3-node NiFi cluster on a local development setup. Because basic authentication does not support multi-node clustering, an external identity provider is necessary to manage user identities across the nodes.

To bypass the typical local network routing challenges—where your browser needs to reach Keycloak at the same hostname that the internal NiFi pods use—you can use an elegant local DNS forwarding strategy.

### Step 1: Deploy Keycloak to Your Local Cluster

Run Keycloak in the same namespace as your NiFi deployment using the official Bitnami chart. Enabling HTTP ensures a simpler setup process for local sandbox environments.

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```
```
helm install keycloak bitnami/keycloak -n nifi `
  --set auth.adminUser=admin `
  --set auth.adminPassword=Password1234 `
  --set production=false `
  --set http.enabled=true `
  --set image.repository=bitnamilegacy/keycloak `
  --set postgresql.image.repository=bitnamilegacy/postgresql
```

### Step 2: Configure Local DNS and Port-Forwarding

NiFi fetches OIDC metadata from the configuration discovery URL and passes those endpoints directly to your browser for redirection. To ensure both the internal cluster pods and your local web browser can resolve the exact same address, follow these two sub-steps:

1. Open your machine's host file (`/etc/hosts` on Linux/macOS or `C:\Windows\System32\drivers\etc\hosts` on Windows) and map the cluster-internal DNS name of Keycloak to your localhost IP address:
```text
127.0.0.1 keycloak.nifi.svc.cluster.local

```


2. Start a persistent port-forwarding session to route traffic from your machine over to the service:
```bash
kubectl port-forward svc/keycloak 80:80 -n nifi
```



### Step 3: Set Up the Client and User in Keycloak

1. Open `http://keycloak.nifi.svc.cluster.local:80` in your web browser and sign in with the credentials `admin` / `Password1234`.
2. Navigate to **Clients** and click **Create client**.
3. Set the **Client ID** to `nifi-client`.
4. On the capability config screen, ensure **Client authentication** is toggled to **On** (Confidential access type).
5. On login settigns, Set the **Valid redirect URIs** to `https://example.com/*` and `https://example.com:8443/nifi-api/access/oidc/callback` (or the specific hostname configured in your Ingress parameters).
6. Save the client configuration, switch over to the **Credentials** tab, and copy the generated **Client Secret** value string.
7. Go to **Users**, click **Add user**, and set the username and email to `admin@company.com`. Under the **Credentials** tab for that user, configure a permanent login password.

### Step 4: Adjust `values.yaml` for Clustering and OIDC

Apply the following modifications to your local configuration file to update the node footprint count, enable the OIDC integration module, and supply your Keycloak parameters.

```diff
diff --git a/values.yaml b/values.yaml
--- a/values.yaml
+++ b/values.yaml
@@ -8,3 +8,3 @@ global:
   nifi:
-    nodeCount: 1  # Default to 1 node since basic auth (default) doesn't support clustering
+    nodeCount: 3  # Default to 1 node since basic auth (default) doesn't support clustering
 
@@ -52,7 +52,7 @@ global:
   oidc:
-    enabled: false
-    oidc_url: ""
-    client_id: ""
-    client_secret: ""
+    enabled: true
+    oidc_url: "http://keycloak.nifi.svc.cluster.local:80/realms/master/.well-known/openid-configuration"
+    client_id: "nifi-client"
+    client_secret: "your-client-secret-copied-from-keycloak"
     client_secretFrom: ""
     claim_identifying_user: "preferred_username"
-    initial_admin_identity: ""
+    initial_admin_identity: "admin@company.com"

```

### Step 5: Install Your Multi-Node Cluster

With ZooKeeper disabled (relying on your modern NiFi 2.x native Kubernetes state strategy) and storage profiles localized, execute the installation tracking command:

```bash
helm install my-nifi apache-nifi-helm -n nifi --create-namespace --set zookeeper.enabled=false

```

### Troubleshooting 
helm uninstall keycloak -n nifi
kubectl delete pvc -l app.kubernetes.io/instance=keycloak -n nifi