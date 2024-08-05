from eqk_function import eqk_function
from emr_function import emr_function

def lambda_handler(event, context):
    emr_result = emr_function()
    
    # 추가 조건을 체크한 후 secondary_function 호출
    if emr_result['event'] == 1 :
        eqk_result = eqk_function()
        return eqk_result

    return emr_result