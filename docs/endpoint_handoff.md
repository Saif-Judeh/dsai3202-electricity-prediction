# Electricity forecasting endpoint handoff

Run these commands from the repository root on the project VM, signed in to
Azure with access to the project workspace.

```bash
rg="rg-dsai3202-saif-60306539"
ws="mlwdsai320260306539"
ep="electricity-60306539"
```

## Before the demo

Check the endpoint state and traffic allocation.

```bash
az ml online-endpoint show -g "$rg" -w "$ws" -n "$ep" \
  --query '{state:provisioning_state,traffic:traffic}' -o json
```

For the live demo, expect `Succeeded` and 100 percent traffic to `blue`.
Send the saved example request.

```bash
az ml online-endpoint invoke -g "$rg" -w "$ws" -n "$ep" \
  --request-file deployment/examples/valid_request.json
```

The expected prediction is approximately **911.483 kWh**.
The full reference and numerical tolerances are in
`deployment/examples/offline_reference.json`.

## Clean up after live testing

When the team is ready to take the endpoint offline, delete the project
endpoint. Check its name at the confirmation prompt.

```bash
az ml online-endpoint delete -g "$rg" -w "$ws" -n "$ep"
az ml online-endpoint list -g "$rg" -w "$ws" \
  --query "[?name=='$ep'].name" -o json
```

An empty list confirms the endpoint is absent. The model asset used for
redeployment is `electricity-catboost`, version `1`.

## Recreate the endpoint for a later demo

First confirm the registered model is available. Then recreate the endpoint
and deploy `blue` using the configuration in this repository.

```bash
az ml model show -g "$rg" -w "$ws" -n electricity-catboost --version 1 \
  --query '{name:name,version:version}' -o json &&
az ml online-endpoint create -g "$rg" -w "$ws" \
  -f deployment/endpoint.yml &&
az ml online-deployment create -g "$rg" -w "$ws" \
  -f deployment/blue.yml --all-traffic
```

Allow time for provisioning before the presentation. Repeat the status check
and example request above before starting the live demo.
