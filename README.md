# Deploy Cognite Function action
This action deploys a Python function to Cognite Functions, possibly with a schedule defined.

## Inputs
### Function metadata in Github Workflow
#### Required
1. `function_name`: Name of your function. That name will become an `external_id` and *must* be unique within your project
2. `function_folder`: Parent folder of function code defaults to `.` (i.e. the root of your repo)
3. `cdf_deployment_credentials`: Name of Github secrets that holds the API-key that shall be used to deploy the function. 
That API-key should have the following CDF capabilities: `Files:READ`, `Files:WRITE`, `Functions:READ`, `Functions:WRITE`
4. `cdf_runtime_credentials`: Name of Github secrets that holds the API-key that the function will use when running
That API-key should have CDF capabilities required to run the code within the Function itself
 
#### Optional
1. `common_folder`:  The path to the folder containing code that is shared between all functions. Defaults to `common/`
1. `cdf_project`: The name of your CDF project/tenant. Inferred from your API-keys. Will be validated with API-keys if provided
2. `cdf_base_url`: Base url of your CDF tenant, defaults to _https://api.cognitedata.com_
3. `function_file`: The name of the file with your main function (defaults to `handler.py`)
4. `function_secrets`: The name of Github secrets that holds the base64 encoded JSON dictionary with secrets. (see secrets section)
5. `schedule_file`: File location with the list of schedules to be applied, see the file format below (defaults to None i.e. no schedules).
6. `remove_only`: Checks that specified function is removed with all its schedules. Deployment logic is skipped

### Schedule file format
```yaml
- name: Daily schedule # that will become part of Schedule's external_id. Has to be unique within the file
  cron: "0 0 * * *"
  data:
    lovely-parameter: True
    something-else: 42
- name: Hourly schedule # that will become part of Schedule's external_id. Has to be unique within the file
  cron: "0 * * * *"
  data:
    lovely-parameter: False
    something-else: 777
```

### Example usage
Workflow:
```yaml
uses: actions/checkout@v2  # Do not forget to check out your own code!
uses: cognitedata/function-action@v2
with:
    function_name: my_hello_function_${{ github.ref }}
    cdf_deployment_credentials: ${{ secrets.COGNITE_DEPLOYMENT_CREDENTIALS }}
    cdf_runtime_credentials: ${{ secrets.COGNITE_FUNCTION_CREDENTIALS }}
    function_file: function1
    function_folder: functions
    common_folder: utilities
    function_secrets: ${{ secrets.COGNITE_FUNCTION_SECRETS }}
    schedule_file: schedule-${{ github.ref }}.yml
```

### Common folder
A common use case is that you do not want to replicate utility code between all function folders. In order to accomodate this, we copy all the contents in the folder specified by `common_folder` into the functions we upload to Cognite Functions. If this is not specified, we check if `common/` exists in the root folder and if so, _we use it_.

#### Handling imports
A typical setup looks like this:
```
├── common
│   └── utils.py
└── my_function
    └── handler.py
```
The code we zip and send off to the FilesAPI will look like this:
```
├── common
│   └── utils.py
└── handler.py
```
This means your `handler.py`-file should do imports from `common/utils.py` like this:
```py
from common.utils import my_helper1, my_helper2
import common.utils as utils  # alternative
```

### Function secrets
When you implement your Cognite Function, you may need to have additional `secrets`, for example if you want to to talk to 3rd party services like Slack.
To achieve this, you could create the following dictionary:
```json
{"slack-token": "123-my-secret-api-key"}
``` 
Use your terminal to encode your credentials into a string:
```shell script
$ echo '{"slack-token": "123-my-secret-api-key"}' | base64 
eyJzbGFjay10b2tlbiI6ICIxMjMtbXktc2VjcmV0LWFwaS1rZXkifQo
```
...or use Python if you don't have `base64` available on your system:
```sh
$ echo '{"slack-token": "123-my-secret-api-key"}' | python -m base64
eyJzbGFjay10b2tlbiI6ICIxMjMtbXktc2VjcmV0LWFwaS1rZXkifQo=
```
To decode and verify it, do:
```sh
$ echo eyJzbGFjay10b2tlbiI6ICIxMjMtbXktc2VjcmV0LWFwaS1rZXkifQo= | python -m base64 -d
{"slack-token": "123-my-secret-api-key"}
```
Take that string and store it into GitHub secret (`COGNITE_FUNCTION_SECRETS`, like in the example above)

Notes: _Keys must be lowercase characters, numbers or dashes (-) and at most 15 characters. You can supply at most 5 secrets in the dictionary (Cognite Functions requirement)_
