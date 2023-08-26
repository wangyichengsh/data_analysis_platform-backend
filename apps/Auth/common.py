import logging
from urllib.parse import unquote
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from itsdangerous import SignatureExpired, BadSignature, BadData
from django.contrib.auth import get_user_model



key = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnGp/Q5lh0P8nPL21oMMrt2RrkT9AW5jgYwLfSUnJVc9G6uR3cXRRDCjHqWU5WYwivcF180A6CWp/ireQFFBNowgc5XaA0kPpzEtgsA5YsNX7iSnUibB004iBTfU9hZ2Rbsc8cWqynT0RyN4TP1RYVSeVKvMQk4GT1r7JCEC+TNu1ELmbNwMQyzKjsfBXyIOCFU/E94ktvsTZUHF4Oq44DBylCDsS1k7/sfZC2G5EU7Oz0mhG8+Uz6MSEQHtoIi6mc8u64Rwi3Z3tscuWG2ShtsUFuNSAFNkY7LkLn+/hxLCu2bNISMaESa8dG22CIMuIeRLVcAmEWEWH5EEforTg+QIDAQAB'

logger = logging.getLogger('GetToken.py')

def token2user(token_list, res='user'):
    secret_key = key
    token_list = unquote(token_list)
    token = token_list.strip().split()[1]
    s = Serializer(secret_key=secret_key)
    try:
        data = s.loads(token)
    except SignatureExpired:
        msg = 'token expired'
        logger.info(msg)
        return None
    except BadSignature as e:
        encoded_payload = e.payload
        if encoded_payload is not None:
            try:
                data = s.load_payload(encoded_payload)
                if res == 'user':
                    User = get_user_model()
                    user_name = data.get('user_name','').split(';')[0]
                    user = User.objects.get(username=user_name)
                    return user
                else:
                    return data
            except BadData:
                msg = 'token tampered'
                logger.info(msg)
                return None
        msg = 'badSignature of token'
        logger.info(msg)
        return None
    except:
        msg = 'wrong token with unknown reason'
        logger.info(msg)
        return None
        
if __name__=="__main__":
    token = 'bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0aW1lQXV0aCI6bnVsbCwiZ3JvdXBOYW1lIjoi56eR5Yib57uEIiwidXNlcl9uYW1lIjoiY2d6aGFuZztDR1pIQU5HO-W8oOS8oOWImiIsInNjb3BlIjpbIm9wZW5pZCJdLCJleHAiOjE1NzQ3NzM5NDMsImF1dGhvcml0aWVzIjpbIlJFQUxfTU9OSVRPUl9HUk9VUF80Il0sImp0aSI6IjM4YjY2NjY2LWNhZmUtNGVhZS04MTM4LWI3YmQwYzRkOGYwNiIsImNsaWVudF9pZCI6Im5nc3AifQ.S6VfCwmWAK3amABxXoKAyLbqBPaQRPPfHZYverabIXJsTI2ndaxoEwXjvaKuMGPB1hfgDuY65AsXaGzdo-m9F_hUtdUqMZAsE70uRTMJX4-BVsiYm7yqoneXxLYmPEFVHQkOzZeyGfFYLsvZElcP-YJBHimgCwf0NSb0EqBCcT-prcV2492DNMxIVT5CpnPQrjWSA1Oz_xRwHvMq-WJ_jRHpIQfZbAcq2IrebKF3JSU1Tst6EehZENBaOSfWSOWjeyi-JqIbVHitvM3T3Expd0Y1cq_6QywglzrbFNg5t9RB99pPPkP1iqWYT2AQ1MHyIa5m33zoWo4qEqoG949V5g'
    print(token2user(token, res='json'))