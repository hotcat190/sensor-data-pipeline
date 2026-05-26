git pull --recursive 

cd apache-nifi-helm

helm dependency update .

helm repo add jetstack https://charts.jetstack.io
helm repo update

helm install cert-manager jetstack/cert-manager \`
  --namespace cert-manager \`
  --create-namespace \`
  --set installCRDs=true

helm install my-nifi . -n nifi --create-namespace