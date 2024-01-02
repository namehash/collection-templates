# Collections preprocessing

Fastapi service providing preprocessing of collections.
- `POST /collections` - endpoint for preprocessing collections
- `wordninja` used for tokenization
- `mangum` used for wrapping FastAPI app and triggering it as lambda handler
- When deploying this lambda with AWS API Gateway set `API_STAGE` 
environment variable ('dev'/'test'/'prod') in Lambda runtime, it will be used as `root_path`; it should have the same value as `api_stage` in the context of AWS API Gateway
  - https://docs.aws.amazon.com/lambda/latest/dg/services-apigateway.html#apigateway-proxy
  - https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-stages.html
  - https://rafrasenberg.com/fastapi-lambda/#Deploying_FastAPI_lambda_with_Terraform

## Example request:
```
{
    "data": [
                {
                    "collection_name": "marvel heroes",
                    "keywords": ["marvel", "superhero"],
                    "names": [
                        "hulk", "CaptainAmerica", "deadpool", "doctorstrange"
                    ],
                    "description": "marvel heroes doing epic things"
                }
    ]
}
```

## Deployment

Deploying lambda function with an API gateway endpoint using aws-cli:

- BUILD & PUSH DOCKER IMAGE
    - `aws ecr create-repository --repository-name fastapi-lambda`
    - `docker buildx build --platform linux/amd64 -t fastapi-lambda --tag <repositoryUri>:latest ./app/
`
    - `aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <accountId>.dkr.ecr.<region>.amazonaws.com`
    - `docker push <repositoryUri>:latest`
- CREATE LAMBDA FUNCTION
    - `aws iam create-role --role-name lambda-apigateway-execution-role --assume-role-policy-document file://deploy/trust-policy.json`
    - `aws lambda create-function --function-name fastapi-lambda --role <execution-role-arn> --package-type Image --code ImageUri=<repositoryUri>:latest`
- CREATE AND CONFIGURE API GATEWAY
    - `aws apigateway create-rest-api --name fastapi-lambda-api-gateway`
    - ``aws apigateway get-resources --rest-api-id <rest-api-id> --query 'items[?path==`/`].id' --output text`` # returns root-resource-id
    - `aws apigateway create-resource --rest-api-id <api-id> --parent-id <root-resource-id> --path-part {proxy+}`
    - `aws apigateway put-method --rest-api-id <api-id> --resource-id <resource-id> --http-method ANY --authorization-type NONE`
    - `aws apigateway put-integration --rest-api-id <api-id> --resource-id <resource-id> --http-method ANY --type AWS_PROXY --integration-http-method POST --uri arn:aws:apigateway:<region>:lambda:path/2015-03-31/functions/arn:aws:lambda:<region>:<accountId>:function:fastapi-lambda/invocations --passthrough-behavior WHEN_NO_MATCH --content-handling CONVERT_TO_BINARY`
    - `aws apigateway put-method --rest-api-id <api-id> --resource-id <root-resource-id> --http-method ANY --authorization-type NONE`
    - `aws apigateway put-integration --rest-api-id <rest-api-id> --resource-id <root-resource-id> --http-method ANY --type AWS_PROXY --integration-http-method POST --uri arn:aws:apigateway:<region>:lambda:path/2015-03-31/functions/arn:aws:lambda:<region>:<accountId>:function:fastapi-lambda/invocations --passthrough-behavior WHEN_NO_MATCH --content-handling CONVERT_TO_BINARY`
    - `aws apigateway create-deployment --rest-api-id <api-id> --stage-name dev`
    - `aws lambda add-permission --function-name fastapi-lambda --statement-id apigateway-invoke --action lambda:InvokeFunction --principal apigateway.amazonaws.com --source-arn "arn:aws:execute-api:<region>:<accountId>:<api-id>/*/*"`

After deployment, docs should be visible at `<api-url>/docs`

Test the deployed api with: 

```
curl -X POST -H "Content-Type: application/json" -d @./deploy/test_request.json https://<api-id>.execute-api.eu-central-1.amazonaws.com/dev/
```
