from datetime import datetime, timedelta
from request import parse

__COOKIE = "GCP_IAP_UID=102309674626857018922; OptanonAlertBoxClosed=2023-02-27T20:06:58.905Z; _ga_ZRKMKEK030=GS1.1.1677532383.1.1.1677532383.0.0.0; intercom-device-id-cytw4cvp=aaa9399f-8760-41c2-8b51-630c8b40929e; _hp2_props.1473692602=%7B%22workspaceId%22%3A%226051921418418893%22%2C%22Experiment%3A%20enableWorkspaceTour%22%3Afalse%2C%22Experiment%3A%20enableHomepagePrescriptiveQuickstart%22%3Atrue%2C%22Experiment%3A%20showAddTeammatesButton%22%3Afalse%2C%22Experiment%3A%20enableOnboardingModal%22%3Afalse%2C%22cloud%22%3A%22AWS%22%2C%22featureTier%22%3A%22ENTERPRISE_TIER_V2%22%2C%22locale%22%3A%22en%22%7D; _db_oauth_obs_prod=BnC1f8yR20y5QynEMZM69fkJqIBK8nRUxBS0o4qoASnfYtKM3h4z4tCrZ2apKbgD0XckjaV5eo9OUz0R2o0qeg3MQBLKJkA8NoN0aIDPuycJGwFkmCBrI7bDaFcay3R8UBiMdgG3x8Xpa_U3m1b_yhs602YdOdo6EiaXKgaCdZGHukhUDWZvtUxtu3DvqahNlxHYrO7_ENLhYwjNOLGWJsXY3TD2bikJOuJNkEIjJyIifQ==|1688581354|h3-isv29jJD5cPhDON3Y09enk8zxtNXjuV3td5uY7Fo=; workspace-url=logfood.cloud.databricks.com; _db_oauth_obs_dev_staging=HIMtp-HyOxC0R-2zlbJ6z14F5Uk_vKocP-7vpOsbrWFkUMDu_aPITYHUAcbktdNzYts4PP1Dw-gJsNKzDD1amdZv2K-RyjCgZDv1PzKCrLpZr5ZdlPyzw3cEadZ97q75JhD3MeY63aZuTILxh4PGIdwXVyGQ4UL61Uqrm6sXdJOkx1Ohw2tr-9h1RBkxhmZz1UW1NA1g1KMipzSlce0XBK0_DFnX2ASU9MDTf6XbKJDwdg==|1689006418|njL7J5TmJ8pOYdrlX43fvHuQPMN2jrwGmSwvWPFnH4c=; _db_oauth_obs=lEMHizBlUXvbZMRTDB4nDSyjJUSH-NViedDIdw8gBW8JliD5oeHK5fbhNsGoexJ1dCogxBcUT5phF9cNE_Xzc39PJfkBh9IYja1kBeDkV9VaytDvq1VXKcXgRLzpS0b4C3U5_Xa02aAmlpr9uS7lsm8pK4SltWG9DUfTG23ti-09rjDf2DmBatN0sckHxpCQ0w8rv1_qLS0=|1689006423|vyl4eX8OvM5CZUKxLoXCFJczZDowmAxFWwnqiIbjxD8=; _hp2_id.1473692602=%7B%22userId%22%3A%226754132459378058%22%2C%22pageviewId%22%3A%225113541489692123%22%2C%22sessionId%22%3A%224244119881048570%22%2C%22identity%22%3A%2244d988de3bad62eced21253f7b539042d44a791de1f3ecc7e7e39731c7c0acc4%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3A%22hashedId%22%2C%22isIdentified%22%3A1%2C%22oldIdentity%22%3Anull%7D; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Jul+11+2023+15%3A23%3A45+GMT-0700+(Pacific+Daylight+Time)&version=202211.2.0&isIABGlobal=false&hosts=&consentId=f3a028bc-af24-474a-bea9-ebcd75e70d5e&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0&geolocation=%3B&AwaitingReconsent=false; security_authentication=Fe26.2**2c8a1c57789b183b3572f097e9d25067e666934a0e4ad08f4793632f5479fc49*0qdWaOFWuTWrBTRR0KOSFQ*cnrrX7rzIfK9K5JftbbgMD9-BKgDXlrJaWWDqH-aaAl7klFrSM1vxq5_9NikTOSWKTpI2qQdRJAG3GK5s-BiPytMrCU8wnsChBtxj8sk-u9RRL0f5euf_pxF6WD8082LilYHXcbWeZ_na4DqfnTGdMfEt_MWsj6Vc-hXuC_wLL39H-4qQ2F_ZbsgieGwjPF3PFobuOpSWKwVL52B-4FO6BMXg4_1I1qCxc6SwchPz7v5tIAVSxMCaWl43apzeq46v-PbscSX_VcAt8L6clQzX10rk_IKCRrroBAfwG5IG6RbeIlKtw3h4sXzLKhBVq5IVQYs7fBaxIqwS5SkhL4LMdXqwlTS_oGe91YkK2197wcZqHs4A9wX9Tih_NsSXauqZZZGT4BEBPUsDoVb_l6iQB0DSGoSTGaLjV3j-ClSY6btl66kQMTlzYSCV1bjSshNLXvXMLY1wNhxir7kt_S_2WgVE7N6B4r05gVwhN36uvpxyvb-zOE0uS5okw5oLbiU5bSNJCmwhZDdT2GPdfcs7eurLAbPBY0mBrxwMCvO4xf68UHKWiQ6KAuJFTRSg-ZTkzuhvESSpx_4-yWMY8vzmdIjKXPuolTai1PkC_EGBN7oeMn8Sy7dQU8Ryowxplym**df456673c2ef7679b0ab4f31ff6f6294fde0582e43575e1cddc0f8585634d8be*0fBD2ctfKXAfsZun_aYskMkSpbRoT9NZBg2aoc9aqAU"

if __name__ == "__main__":
    endDateStr = input("Enter end date: (yyyy-mm-dd):")
    timeDelta = input("Enter date delta: (int):")

    if endDateStr is None or endDateStr.strip() == "":
        endTimestamp = datetime.today()
    else:
        endTimestamp = datetime.strptime(endDateStr, '%Y-%m-%d')

    if timeDelta is None or timeDelta.strip() == "":
        timeDelta = 1
    startTimestamp = (endTimestamp - timedelta(days=timeDelta))

    parse(cookie=__COOKIE, startTimestamp=startTimestamp, endTimestamp=endTimestamp)
