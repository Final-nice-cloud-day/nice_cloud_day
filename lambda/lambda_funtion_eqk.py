from typing import Any, Dict

from aws_lambda_typing import Context
from emr_function import emr_function
from eqk_function import eqk_function


def lambda_handler(event: Dict[str, Any], context: Context) -> Dict[str, Any]:
    emr_result = emr_function()

    # 추가 조건을 체크한 후 secondary_function 호출
    if emr_result["event"] == 1 :
        eqk_result = eqk_function()
        return eqk_result

    return emr_result
