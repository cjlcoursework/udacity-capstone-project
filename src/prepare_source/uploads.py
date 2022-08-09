import boto3

ssm_client = boto3.client('ssm')


def list_parameters():
    # creating paginator object for describe_parameters() method
    paginator = ssm_client.get_paginator('describe_parameters')

    # creating a PageIterator from the paginator
    page_iterator = paginator.paginate().build_full_result()

    # loop through each page from page_iterator
    for page in page_iterator['Parameters']:
        if str(page['Name']).startswith("/udacity"):
            response = ssm_client.get_parameter(
                Name=page['Name'],
                WithDecryption=True
            )

            value = response['Parameter']['Value']
            print(f"""'{page['Name']}': '{value}', """)


def get_parameter(name: str):
    response = ssm_client.get_parameter(
        Name=name,
        WithDecryption=True
    )
    value = response['Parameter']['Value']
    print("FOUND: Parameter Name: " + name + " == " + value)


if __name__ == "__main__":
    list_parameters()
    get_parameter("/udacity/dev/bucket/raw_data")
    get_parameter("/udacity/dev/bucket/processed_data")
