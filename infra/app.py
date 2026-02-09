#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stack import AdsbProcessingStack

app = cdk.App()
AdsbProcessingStack(app, "AdsbProcessingStack", env=cdk.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"],
))
app.synth()
