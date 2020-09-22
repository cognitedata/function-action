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

#### Example usage to deploy
Workflow:
```yaml
uses: cognitedata/function-action@v1
env:
  COGNITE_DEPLOYMENTS_CREDENTIALS: ${{ secrets.COGNITE_DEPLOYMENT_CREDENTIALS }}
  COGNITE_FUNCTION_CREDENTIALS: ${{ secrets.COGNITE_FUNCTION_CREDENTIALS }}
with:
  function_name: function1
  config_file_path: ./config.yaml
```

`config.yaml`:
```yaml
functions:
  function1:
    folder_path: "function1"
    file: "handler.py"
    schedules:
      - cron: "*/5 * * * *"
        name: "Run every 5 minutes"
    tenants:
      - cdf_project: "cognite"
        deployment_key_name: "COGNITE_DEPLOYMENT_CREDENTIALS"
        function_key_name: "COGNITE_FUNCTION_CREDENTIALS"
        cdf_base_url: "https://api.cognitedata.com"
```

#### Example usage to delete
Workflow:
```yaml
uses: cognitedata/function-action@v1
env:
  COGNITE_DEPLOYMENTS_CREDENTIALS: ${{ secrets.COGNITE_DEPLOYMENT_CREDENTIALS }}
  COGNITE_FUNCTION_CREDENTIALS: ${{ secrets.COGNITE_FUNCTION_CREDENTIALS }}
  DELETE_PR_FUNCTION: "true"
with:
  function_name: function1
  config_file_path: ./config.yaml
```


### Function metadata in Github Workflow
1. `cdf_project`: Name of your CDF project/tenant
2. `cdf_deployment_credentials`: Name of Github secrets that holds the API-key that shall be used to deploy the function
3. `cdf_function_credentials`: Name of Github secrets that holds the API-key that the function will use when running
4. `cdf_base_url`: Base url of your CDF tenant, defaults to _https://api.cognitedata.com_
5. `function_file`: Name of the main function python file (usually `handler.py`)
6. `function_folder`: Parent folder of function code, defaults to `.` (i.e. the root of your repo)


#### Example usage
Workflow:
```yaml
uses: cognitedata/function-action@v1
with:
    cdf_project: cognite
    cdf_deployment_credentials: ${{ secrets.COGNITE_DEPLOYMENT_CREDENTIALS }}
    cdf_function_credentials: ${{ secrets.COGNITE_FUNCTION_CREDENTIALS }}
    function_file: function1
    function_folder: ./functions
```
