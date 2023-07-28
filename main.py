from datetime import datetime, timedelta

from request import parse, parseTestData, writeTestData

__COOKIE = "GCP_IAP_UID=102309674626857018922; OptanonAlertBoxClosed=2023-02-27T20:06:58.905Z; _ga_ZRKMKEK030=GS1.1.1677532383.1.1.1677532383.0.0.0; intercom-device-id-cytw4cvp=aaa9399f-8760-41c2-8b51-630c8b40929e; _hp2_props.1473692602=%7B%22workspaceId%22%3A%226051921418418893%22%2C%22Experiment%3A%20enableWorkspaceTour%22%3Afalse%2C%22Experiment%3A%20enableHomepagePrescriptiveQuickstart%22%3Atrue%2C%22Experiment%3A%20showAddTeammatesButton%22%3Afalse%2C%22Experiment%3A%20enableOnboardingModal%22%3Afalse%2C%22cloud%22%3A%22AWS%22%2C%22featureTier%22%3A%22ENTERPRISE_TIER_V2%22%2C%22locale%22%3A%22en%22%7D; workspace-url=logfood.cloud.databricks.com|logfood-us-west-2-mt.cloud.databricks.com|oportun-prod.cloud.databricks.com|billing-workspace-mt-prod-aws-us-west-2.cloud.databricks.com|billing-workspace-mt-prod-aws-eu-central-1.cloud.databricks.com; _db_oauth_obs_dev_staging=iYuqoAcy3hUmP_hG076joTvcNllj8beAtiQ7EdX_rWJiSt90vM1ygcQMUSARpyu0mbslaG74DNSABwhbgpXIJhuEqMnV0VytsQ1oDXKhlMaLxu_6zX_r7kR-sGD7lPUHopyL2P4opZSjuOkVQH4eB3J5t6woVnu3CeRldFLRDTEcwiWpmb9L2lc-e-wgVV871gRIJKwNwOcMBOHGHK8NBghYk4_tLbab3J28JEHJzAxFAQ==|1690301007|F-if7Ql6b7I5cbnNovyHRflbbOvYzcBJt2Ya94w_HtE=; _db_oauth_obs=jSbAtwVB-hBN5QoCHQ8c1xLEo39AbUQTMvop5qKTlpfVZaA2kmZPX_xnnSEYEElkmmJTXY30gZ8uYIIzwuPQBYN7aC8BM-B7vmgTMv90yliZ4ltKGbYmmsmSIxm4v8aSTbr3cr65uzW1qV-ZOCHUpxA3x_Un6-2gcA728KszJQ1RxgGIeLv61YITtWP29gRAtmllwPXsKhPx|1690301017|28Bq-1oIs3W6oKrXNpOvrS3DZDc4-vrDvr3D29id6G4=; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Jul+26+2023+23%3A52%3A51+GMT-0700+(Pacific+Daylight+Time)&version=202211.2.0&isIABGlobal=false&hosts=&consentId=f3a028bc-af24-474a-bea9-ebcd75e70d5e&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0&geolocation=%3B&AwaitingReconsent=false; _hp2_id.1473692602=%7B%22userId%22%3A%226754132459378058%22%2C%22pageviewId%22%3A%225566970545385505%22%2C%22sessionId%22%3A%223307357007941656%22%2C%22identity%22%3A%2244d988de3bad62eced21253f7b539042d44a791de1f3ecc7e7e39731c7c0acc4%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3A%22hashedId%22%2C%22isIdentified%22%3A1%2C%22oldIdentity%22%3Anull%7D; _db_oauth_obs_prod=E2t71y9wROBaSJcp-UJdeXgd6WcP1GNgpVNrhZli1_HtUzJcSoLplcICLBoq5_SoNVbl3g4TUYFRiRXJhIE5qUlYiZ72KPE7l0Fg1hFnnG-ZBigwX8TXZSbzJaXIvPPttIFy4IvK7Ik5p29Tv-Dwbhze7UFE7o5tS4ltkdXKS2QGMCy1OKt6po-1BnE1FzXRtvKoSgYSpbIAsz65meHAKYEAaf5LxMHYXfZARU7lnaquNQ==|1690447363|SmX_wE5aR7FAoxr2wguP01cB4TppkG6fE390zSPu3r4=; security_authentication=Fe26.2**c52dd6300f1970d65c5e17ef7e5a61fb71d0eb2c75d39c71ae6185c681f9fc40*7A2Hjui5coVXuvLii5nd1A*XjVyGTM6AFan9TdKt_kprt2GAQVfg3soduBGB07rNzuzpKKywZTz_9obsDWCLZgK6K6Lk3kH9a--q_4uwHMq_jHKFc7NZGzYp9-2efdlb_B2KvIG60F3KZIEkLVSfNYAq_at8zU-jjojucDvbqNCVbR8fjdYlJPnrXu0kAW8vUaW9aI8DtH8aMGSJ_vgSEPFNrcG2XhAWHQhctrpfgwIsz9JeUZCP7fOnn1x1wwRrBO0mRpppB-HMxVm8yLdQN0s0GvgDrwWfDE3gCrkv1HwzxXRIPCYtleizTCejCTXnHTemj7humzx1JCo4qfP1E5lXp4rihLnRQYh3DAk-xFDOiWWnu6jKIkoH4CLNp8t4W_MzcDodtWmelSv9poWZDEcvpZEIClw9FFwHWLDsgbxewuj07_lu8UK_Jl_pzIIfRQRkS593xYVI5ZTqcyYD7wYIGOGlQhGhfnXt4RTIjT9mVaPgNKAGZFn9G0aB_roRL-2trPyPnPr7FP9HZYC9mNLEH8dwYBERqc1RKCblO1uc5bJrOLdp0wEXMbq3QF1x73qmMLKK2XEz7floO_u95gfkCn4ZMRFBu3jaILDpG8vjTe8XqHEk_mzBLyJoNHXYY8gIwB9w5-ZSliVbvPCPQbl**d2a6eecc2541411ce2a6a2633164df74f798950940fbb08396f870ac8c2ffd12*_elHZjF7zsdONvk-Rt1i4FxFbtHGAWLktuHwRkbgKbo"

if __name__ == "__main__":
    endDateStr = input("Enter end date: (yyyy-mm-dd):")
    timeDelta = input("Enter date delta: (int):")
    mode = input("Enter the mode (default to prod):")

    if endDateStr is None or endDateStr.strip() == "":
        endTimestamp = datetime.today()
    else:
        endTimestamp = datetime.strptime(endDateStr, '%Y-%m-%d')

    if timeDelta is None or timeDelta.strip() == "":
        timeDelta = 1
    startTimestamp = (endTimestamp - timedelta(days=int(timeDelta)))

    if mode is None or mode.strip() == "prod":
        parse(cookie=__COOKIE, startTimestamp=startTimestamp, endTimestamp=endTimestamp)
    elif mode.strip() == "writeTestData":
        writeTestData(cookie=__COOKIE, startTimestamp=startTimestamp, endTimestamp=endTimestamp)
    elif mode.strip() == "test":
        parseTestData()
    else:
        raise Exception(f"invalid mode {mode}")
