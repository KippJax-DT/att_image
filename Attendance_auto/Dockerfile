FROM public.ecr.aws/lambda/python:3.11

RUN pip install awscli
RUN aws configure set aws_access_key_id ${access}
RUN aws configure set aws_secret_access_key ${secret_key}
RUN aws configure set region us-east-1
RUN aws configure set output json

COPY requirements.txt ${LAMBDA_TASK_ROOT}
COPY app.py ${LAMBDA_TASK_ROOT}

RUN python3.11 -m pip install -r requirements.txt -t .

# Command can be overwritten by providing a different command in the template directly.
CMD ["app.lambda_handler"]
