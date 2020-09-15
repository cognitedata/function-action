# Deploy Cognite Function action
This action deploys a Python function to Cognite Functions.
There are 2 ways to use this action:
1. Using a configuration file (.yaml format)
2. Defining your function metadata directly in your Github workflow

## Inputs
### Configuration file
If you're using a configuration file you'll need to define the following arguments in your Github workflow:
1. `function_name`: Name of the function to deploy. This value must match a value in your configuration file.
2. `file_path`: Path to your configuration file.

#### Example usage
Workflow:
```yaml
uses: andeplane/deploy-function-python
env:
  COGNITE_DEPLOYMENTS_CREDENTIALS: ${{ secret.COGNITE_DEPLOYMENT_CREDENTIALS }}
  COGNITE_FUNCTION_CREDENTIALS: ${{ secret.COGNITE_FUNCTION_CREDENTIALS }}
with:
  function_name: function1
  file_path: ./config.yaml
```

`config.yaml`:
```yaml
function_folder: "."
functions:
  function1:
    path: "function1"
    schedule:
      cron: "*/5 * * * *"
      name: "Run every 5 minutes"
    tenants:
      - cdf_project: "cognite"
        deployment_key_name: "COGNITE_DEPLOYMENT_CREDENTIALS"
        function_key_name: "COGNITE_FUNCTION_CREDENTIALS"
        cdf_base_url: "https://api.cognitedata.com"
```


### Function metadata in Github Workflow
1. `cdf_project`: Name of your CDF project/tenant
2. `cdf_deployment_credentials`: Name of Github secret that holds the API-key that shall be used to deploy the function
3. `cdf_function_credentials`: Name of Github secret that holds the API-key that the function will use when running
4. `cdf_base_url`: Base url of your CDF tenant, defaults to _https://api.cognitedata.com_
5. `function_path`: Path of the function to deploy (relative to the value set for `function_folder`)
6. `function_folder`: Parent folder of function code, defaults to `.` (i.e. the root of your repo)


#### Example usage
Workflow:
```yaml
uses: cognitedata/deploy-function-action
with:
    cdf_project: cognite
    cdf_deployment_credentials: ${{ secret.COGNITE_DEPLOYMENT_CREDENTIALS }}
    cdf_function_credentials: ${{ secret.COGNITE_FUNCTION_CREDENTIALS }}
    function_path: function1
    function_folder: ./functions
```
