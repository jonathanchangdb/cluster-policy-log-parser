from datetime import datetime, timedelta

from request import parse, parseTestData, writeTestData

__COOKIE = "GCP_IAP_UID=102309674626857018922; OptanonAlertBoxClosed=2023-02-27T20:06:58.905Z; _ga_ZRKMKEK030=GS1.1.1677532383.1.1.1677532383.0.0.0; intercom-device-id-cytw4cvp=aaa9399f-8760-41c2-8b51-630c8b40929e; _hp2_props.1473692602=%7B%22workspaceId%22%3A%226051921418418893%22%2C%22Experiment%3A%20enableWorkspaceTour%22%3Afalse%2C%22Experiment%3A%20enableHomepagePrescriptiveQuickstart%22%3Atrue%2C%22Experiment%3A%20showAddTeammatesButton%22%3Afalse%2C%22Experiment%3A%20enableOnboardingModal%22%3Afalse%2C%22cloud%22%3A%22AWS%22%2C%22featureTier%22%3A%22ENTERPRISE_TIER_V2%22%2C%22locale%22%3A%22en%22%7D; _hp2_id.1473692602=%7B%22userId%22%3A%226754132459378058%22%2C%22pageviewId%22%3A%224941252985132794%22%2C%22sessionId%22%3A%228820604385619561%22%2C%22identity%22%3A%2244d988de3bad62eced21253f7b539042d44a791de1f3ecc7e7e39731c7c0acc4%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3A%22hashedId%22%2C%22isIdentified%22%3A1%2C%22oldIdentity%22%3Anull%7D; _db_oauth_obs_dev_staging=I00NwqyR1BchEsnbCTwEBqktsm9F7JLEtiX1UO4JAd-wIm2AZVnVjBjNDrIfVLTHix5QA23dgInuhwFBQn-rg2B4WJqeVIeCNa_jdrI2EJf2QhdT9K4jEkA_S8ojCmjVbqiePxWRWcUN9nupYU4PJnKyzKwMiwA3br_VMWG4eFcOum6EP7aUHIG5C62zGL0aHM3CDOSYpi0mvdIu2MvP51kkwP98yyPeWu6VmFgu64bgTw==|1690910942|Ve1Al635YgxqE2vw9ILVxuxBFyTdeZWD_789cMjvchU=; _db_oauth_obs=pNEx8m-ocbgtVPjMR01-HIigRmHONDLd59XxB2XQq-f4ldHMFuIqRzjK2OFN26rg5lm2G1KbwOuTfgogS4AIL05HYkSOwOcmyu_uMrolj0BC8WIia9CJa9q72HcZg659q-h-dDtTTEA8OajyM5Ww-eDCwNvSqjZ7LpybW6Z35u1IShxBewtCZPg2QiEKwRnppvnqVJxn0-s=|1690910945|QxxsZMckOU6Jns4_RjFXn-QJZ4t2ciwuwD81Aqd0byc=; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Aug+02+2023+20%3A18%3A19+GMT-0700+(Pacific+Daylight+Time)&version=202211.2.0&isIABGlobal=false&hosts=&consentId=f3a028bc-af24-474a-bea9-ebcd75e70d5e&interactionCount=5&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0&geolocation=%3B&AwaitingReconsent=false; _db_oauth_obs_prod=1VMx_IjkttmJeXI0hg__4a0kyXzPFxvHoeruKQyaK1X5pzuXkuhYU-VfFJ0h9XzVJCD4sEzjB-u9HBDyhRcSUFBOG-f2aU9-Vltaixbj1PK_sDmxRBkhj9pcLafaZdG6XcQwxGk7cHTvmWIMSqHJ4bbH7WV4eolORRM8ntWQafgl3uIc2xa5Dsn_gDSiyvdVQvxu3-Nnl_Gqr2jrOohoWk6cwDe3seeSzL9R0YixnzTFyA==|1691085735|fMMkjT6h-ZQII8k8v6gkkXsBz4_2cvYuaF0thQMbSQo=; workspace-url=logfood.cloud.databricks.com|logfood-us-west-2-mt.cloud.databricks.com|oportun-prod.cloud.databricks.com|billing-workspace-mt-prod-aws-us-west-2.cloud.databricks.com|billing-workspace-mt-prod-aws-eu-central-1.cloud.databricks.com|logfood-us-east-1.cloud.databricks.com|retool-databricks-production.cloud.databricks.com; _hp2_props.3428506230=%7B%22cloud%22%3A%22AWS%22%2C%22featureTier%22%3A%22STANDARD_W_SEC_TIER%22%2C%22locale%22%3A%22en%22%2C%22workspaceId%22%3A%223665923216268002%22%2C%22Experiment%3A%20enableWorkspaceTour%22%3Atrue%2C%22Experiment%3A%20enableHomepagePrescriptiveQuickstart%22%3Afalse%2C%22Experiment%3A%20showAddTeammatesButton%22%3Afalse%2C%22Experiment%3A%20enableOnboardingModal%22%3Atrue%7D; _hp2_ses_props.3428506230=%7B%22r%22%3A%22https%3A%2F%2Fgenie.cloud.databricks.com%2F%22%2C%22ts%22%3A1691167674423%2C%22d%22%3A%22retool-databricks-production.cloud.databricks.com%22%2C%22h%22%3A%22%2F%22%2C%22q%22%3A%22%3Fo%3D3665923216268002%22%7D; _hp2_id.3428506230=%7B%22userId%22%3A%228957466371374172%22%2C%22pageviewId%22%3A%224449444442809211%22%2C%22sessionId%22%3A%223411174922730781%22%2C%22identity%22%3A%22e68f9d222fc2b7f34e0c01c19e1b9fc818af9947715836a930920eb68f13c473%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3A%22hashedId%22%2C%22isIdentified%22%3A1%7D; __cf_bm=JK7BK27xgfA93R6wImTMmgg_ouebjUnyA36bVh.M4uw-1691169188-0-AbKBJshZ2gOKNvaj+wDphe96PgkChUokdMX3gife5bS3Tvi/OxGENg+u9DyDUeWDY7vmHJEb3ugnXLgbIdAly8Q=; security_authentication=Fe26.2**306024018f2848df461eaf211f9abde9aca830760d61d5e5af13ed7da96778d9*hCFUx6_JPNqcZfPckZDIPQ*1LSoVCFCnQOgsqTBiu-Kw8upsY9owL9m_HL7xd5rhQxgv1M0p2QFJtAblezPdylt9_4h2y5V_eZ7IOcbXclkgJW2C1srykMINiS7fNrIDcAI4fRtWLqVnpwMd-vv0fx5q08fcyVZ8ETBfnzzvzkVpTRUmuPsIlK9qSdxhEGUO0iXqhWSIggpeDQszrgenFUro7CiLn1yaTDl2QbuwbPkHGlnENabRnwMjUePS1rlqJ-cDsIoVggClRmiwocSVoJ0ULTr9qvqZe9sZSL4Ail-nTqcA7xvJUU5wB4OGZsqhnws_AXMmQBAjx8mkDY4gzb3cDNINteqidlRLK3CDqwP3-fqtH82So9tGsSfA7nrns49ax47RBX0_WzOCR9ZSgtQBry6Y1czyfyUkKBfRWMzY-p2Qes54snvXPIEdyoJ_rmqEHqvgkdjFxMO1e7fjxoM1uL8M6oB2P0BHFKEmDPOJ8s2HzB859K5clZFFhhWAfrNU8tMO6kFfxAvk-JWpztpwW9cKQabFwwCiCBMASgVp_1Qsq_mQBZnNrzy-_sHNIHqVklXyfSOYwW9NyQxMM0OTmFdRs4WnNGwYWnXYVeK5bKonbbdjFsXLarQROlQpHINJ95wak-v2-6D0iOSNi4T**cb1936368678bbdd1c98ddb4995c14648265528ba96034ff9e0d560c8b8cee35*JPbSuPSc63pvvof-8HsRpQWeDR_-MgfyZJkYony9Z5U"

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
