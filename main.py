from datetime import datetime, timedelta

from request import parse

__COOKIE = "GCP_IAP_UID=102309674626857018922; OptanonAlertBoxClosed=2023-02-27T20:06:58.905Z; _ga_ZRKMKEK030=GS1.1.1677532383.1.1.1677532383.0.0.0; intercom-device-id-cytw4cvp=aaa9399f-8760-41c2-8b51-630c8b40929e; _hp2_props.1473692602=%7B%22workspaceId%22%3A%226051921418418893%22%2C%22Experiment%3A%20enableWorkspaceTour%22%3Afalse%2C%22Experiment%3A%20enableHomepagePrescriptiveQuickstart%22%3Atrue%2C%22Experiment%3A%20showAddTeammatesButton%22%3Afalse%2C%22Experiment%3A%20enableOnboardingModal%22%3Afalse%2C%22cloud%22%3A%22AWS%22%2C%22featureTier%22%3A%22ENTERPRISE_TIER_V2%22%2C%22locale%22%3A%22en%22%7D; workspace-url=logfood.cloud.databricks.com; _db_oauth_obs_dev_staging=HIMtp-HyOxC0R-2zlbJ6z14F5Uk_vKocP-7vpOsbrWFkUMDu_aPITYHUAcbktdNzYts4PP1Dw-gJsNKzDD1amdZv2K-RyjCgZDv1PzKCrLpZr5ZdlPyzw3cEadZ97q75JhD3MeY63aZuTILxh4PGIdwXVyGQ4UL61Uqrm6sXdJOkx1Ohw2tr-9h1RBkxhmZz1UW1NA1g1KMipzSlce0XBK0_DFnX2ASU9MDTf6XbKJDwdg==|1689006418|njL7J5TmJ8pOYdrlX43fvHuQPMN2jrwGmSwvWPFnH4c=; _db_oauth_obs=lEMHizBlUXvbZMRTDB4nDSyjJUSH-NViedDIdw8gBW8JliD5oeHK5fbhNsGoexJ1dCogxBcUT5phF9cNE_Xzc39PJfkBh9IYja1kBeDkV9VaytDvq1VXKcXgRLzpS0b4C3U5_Xa02aAmlpr9uS7lsm8pK4SltWG9DUfTG23ti-09rjDf2DmBatN0sckHxpCQ0w8rv1_qLS0=|1689006423|vyl4eX8OvM5CZUKxLoXCFJczZDowmAxFWwnqiIbjxD8=; _db_oauth_obs_prod=C9HTq2lRUSi-KaTTg-vozLtUfTdCwJbh4jmfjknygEMqYcLTREQ0M-JuXt3bR8Qk2Pk95qQdxSAvqgYFZg2KhkgxFiZXop89ZfX7wb6e2TDUhVmKGP1quY87gf7GdZqXLC74DSPp6M_Mwnc90HKVSnjTX35Sdj3pd-mmrHUEPHq7cdqPafCZGuzrocUakETBPFNR6U83hKmznqRdHW5FH_N2Z_4vvWyg1Sk-Mz4BGWTIAQ==|1689188005|Ee5eFVXF_G_SmfvtAxKgKBYtayFt87m6UfjH2emUzlg=; locale=en; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Jul+12+2023+16%3A43%3A26+GMT-0700+(Pacific+Daylight+Time)&version=202211.2.0&isIABGlobal=false&hosts=&consentId=f3a028bc-af24-474a-bea9-ebcd75e70d5e&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0&geolocation=%3B&AwaitingReconsent=false; _hp2_id.1473692602=%7B%22userId%22%3A%226754132459378058%22%2C%22pageviewId%22%3A%228736637996894379%22%2C%22sessionId%22%3A%225708962343371307%22%2C%22identity%22%3A%2244d988de3bad62eced21253f7b539042d44a791de1f3ecc7e7e39731c7c0acc4%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3A%22hashedId%22%2C%22isIdentified%22%3A1%2C%22oldIdentity%22%3Anull%7D; security_authentication=Fe26.2**954e822c08d841a28011db9d27fa942a21a90c0eafedcdee74eb3a15823f30e4*Hzn3uU_g0BvUyXdxBCf6EA*iOCVUJAXCX6Yjv1AgSL-a4UU5FsJQtxCXYSnAz_l4osDF40yEpmLaF1lsMRz2oHmZHIeMSsjk2tbGIh4cRgYtSEqIe3OFTMoHlGtIsn7Zw4dZ009Chuq0pjTZ2s1-jP9jSd6p1qUKtOgv5OjhAjuFyVfI1-UCfJ3vmcKzY4m5IU7kg-UxY2aIDd-XZOI7TAL6W5WlQZUi0u9ps5_m0UJ9-CAsMfFWso-Usz0aBgO8zY-dWBBi_z7U4YXUD9MVt9LQQUsMVqaYCcp81SigCrnPIv6Ced54Y1TQ9ZxZo4a5v6gRo6rnIY8UuXGwIwqFz3tD9jgYHvjCnuWP9Lfpi4kRO4jFWGtIsMz9ottIzq8trXzIkV7hlPxSGX7rqb34ehg-0NEt-ywuJZkw357zQouIcnrbGGQd0rnB_ljSQMcXBqJhqsAGbWTO34D5H0CGCD7zavDOgfnW-LveGb-kKJDV-4zY4nkL_J6TqTEnRcf0WymfW4bDeSFshJp214O91STdED6C9kDkrbNOzUpNJe6elPoWJOkllxZP9a_sPQGUsx1uzZD04zGN-mbQfMah2hBUqAX5RL-vVY1nz4dZyKkgyj5TeNunM7d3Zc3QvASlcDLKEn4D4YOl3LUK7fDEwk4**ced45e0e705b8a19f0135b2bbdb6a4ed5868e1920a26ee6744a7ea5c1e7ccfb4*W8ecFzeiMdBxtoXDr-wg0xbkIciURZLUTkVqUZrd8bw"

if __name__ == "__main__":
    endDateStr = input("Enter end date: (yyyy-mm-dd):")
    timeDelta = input("Enter date delta: (int):")

    if endDateStr is None or endDateStr.strip() == "":
        endTimestamp = datetime.today()
    else:
        endTimestamp = datetime.strptime(endDateStr, '%Y-%m-%d')

    if timeDelta is None or timeDelta.strip() == "":
        timeDelta = 1
    startTimestamp = (endTimestamp - timedelta(days=int(timeDelta)))

    parse(cookie=__COOKIE, startTimestamp=startTimestamp, endTimestamp=endTimestamp)
