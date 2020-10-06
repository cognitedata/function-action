# Deploy Cognite Function action
This action deploys a Python function to Cognite Functions with schedule defined.

## Inputs
### Function metadata in Github Workflow
#### Required
1. `function_name`: Name of your function. That name will become an `external_id` and *must* be unique within your project
2. `function_folder`: Parent folder of function code, defaults to `.` (i.e. the root of your repo)
3. `cdf_deployment_credentials`: Name of Github secrets that holds the API-key that shall be used to deploy the function. 
That API-key should have following CDF capabilities: `Files:READ`, `Files:WRITE`, `Functions:READ`, `Functions:WRITE`
4. `cdf_runtime_credentials`: Name of Github secrets that holds the API-key that the function will use when running
That API-key should have CDF capabilities required to run the code within the Function itself
 
#### Optional
1. `cdf_project`: Name of your CDF project/tenant. Inferred from your API-keys. Will be validated with API-keys if provided
2. `cdf_base_url`: Base url of your CDF tenant, defaults to _https://api.cognitedata.com_
3. `function_file`: Name of the main function python file (defaults to `handler.py`)
4. `schedules`: List of CronTab schedules function should be triggered with. Json encoded string `['* * * * *', '*/15 * * * *']`.
Defaults to `[]` (no schedules)
5. `remove_only`: Checks that specified function is removed with all it's schedules. Deployment logic is skipped

#### Example usage
Workflow:
```yaml
uses: cognitedata/function-action@v2
with:
    function_name: my_hello_function_${{ github.ref }}
    cdf_deployment_credentials: ${{ secrets.COGNITE_DEPLOYMENT_CREDENTIALS }}
    cdf_runtime_credentials: ${{ secrets.COGNITE_FUNCTION_CREDENTIALS }}
    function_file: function1
    function_folder: ./functions
    schedules: "['*/15 * * * *']"
```
