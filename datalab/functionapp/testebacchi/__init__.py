import azure.functions as func


def main(req: func.HttpRequest) -> str:
    return str(req.get_json())
