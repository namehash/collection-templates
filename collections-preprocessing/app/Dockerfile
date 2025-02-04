FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN python3 -m ensurepip
RUN pip install -r requirements.txt

COPY src ${LAMBDA_TASK_ROOT}

ENV PYTHONPATH "${PYTHONPATH}:${LAMBDA_TASK_ROOT}"

# set appropriate api stage
ENV API_STAGE "dev"

CMD [ "main.handler" ]
