from datetime import datetime, timedelta

from request import parse

__COOKIE = "GCP_IAP_UID=102309674626857018922; OptanonAlertBoxClosed=2023-02-27T20:06:58.905Z; _ga_ZRKMKEK030=GS1.1.1677532383.1.1.1677532383.0.0.0; intercom-device-id-cytw4cvp=aaa9399f-8760-41c2-8b51-630c8b40929e; _hp2_props.1473692602=%7B%22workspaceId%22%3A%226051921418418893%22%2C%22Experiment%3A%20enableWorkspaceTour%22%3Afalse%2C%22Experiment%3A%20enableHomepagePrescriptiveQuickstart%22%3Atrue%2C%22Experiment%3A%20showAddTeammatesButton%22%3Afalse%2C%22Experiment%3A%20enableOnboardingModal%22%3Afalse%2C%22cloud%22%3A%22AWS%22%2C%22featureTier%22%3A%22ENTERPRISE_TIER_V2%22%2C%22locale%22%3A%22en%22%7D; workspace-url=logfood.cloud.databricks.com; _db_oauth_obs_dev_staging=HIMtp-HyOxC0R-2zlbJ6z14F5Uk_vKocP-7vpOsbrWFkUMDu_aPITYHUAcbktdNzYts4PP1Dw-gJsNKzDD1amdZv2K-RyjCgZDv1PzKCrLpZr5ZdlPyzw3cEadZ97q75JhD3MeY63aZuTILxh4PGIdwXVyGQ4UL61Uqrm6sXdJOkx1Ohw2tr-9h1RBkxhmZz1UW1NA1g1KMipzSlce0XBK0_DFnX2ASU9MDTf6XbKJDwdg==|1689006418|njL7J5TmJ8pOYdrlX43fvHuQPMN2jrwGmSwvWPFnH4c=; _db_oauth_obs=lEMHizBlUXvbZMRTDB4nDSyjJUSH-NViedDIdw8gBW8JliD5oeHK5fbhNsGoexJ1dCogxBcUT5phF9cNE_Xzc39PJfkBh9IYja1kBeDkV9VaytDvq1VXKcXgRLzpS0b4C3U5_Xa02aAmlpr9uS7lsm8pK4SltWG9DUfTG23ti-09rjDf2DmBatN0sckHxpCQ0w8rv1_qLS0=|1689006423|vyl4eX8OvM5CZUKxLoXCFJczZDowmAxFWwnqiIbjxD8=; _db_oauth_obs_prod=C9HTq2lRUSi-KaTTg-vozLtUfTdCwJbh4jmfjknygEMqYcLTREQ0M-JuXt3bR8Qk2Pk95qQdxSAvqgYFZg2KhkgxFiZXop89ZfX7wb6e2TDUhVmKGP1quY87gf7GdZqXLC74DSPp6M_Mwnc90HKVSnjTX35Sdj3pd-mmrHUEPHq7cdqPafCZGuzrocUakETBPFNR6U83hKmznqRdHW5FH_N2Z_4vvWyg1Sk-Mz4BGWTIAQ==|1689188005|Ee5eFVXF_G_SmfvtAxKgKBYtayFt87m6UfjH2emUzlg=; locale=en; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Jul+12+2023+16%3A43%3A26+GMT-0700+(Pacific+Daylight+Time)&version=202211.2.0&isIABGlobal=false&hosts=&consentId=f3a028bc-af24-474a-bea9-ebcd75e70d5e&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0&geolocation=%3B&AwaitingReconsent=false; _hp2_id.1473692602=%7B%22userId%22%3A%226754132459378058%22%2C%22pageviewId%22%3A%228736637996894379%22%2C%22sessionId%22%3A%225708962343371307%22%2C%22identity%22%3A%2244d988de3bad62eced21253f7b539042d44a791de1f3ecc7e7e39731c7c0acc4%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3A%22hashedId%22%2C%22isIdentified%22%3A1%2C%22oldIdentity%22%3Anull%7D; security_authentication=Fe26.2**a4245d04f9dae3273610cff19dd118c05ca499b4d92d16bbce9a6842a4f0ea53*y4f16M9g3DjWxarVVJdXbw*4bfEtHw75M40t1wwYZrdYUbAAV4Jv1GT5zXXh3BsWPDLHRrYtjMm9-6SkSfN8s4_6u_woxMLH4LkGBUFLwS5Wxg2PmXU2Uz5lelgVBPP4m7reN1eKimT3pv5ME9e30OVMEO_UYCjkfIQiXjBTQr31PSUO5Xubk6AIrNZp0nPIDptghFb2w8UjU9xItPuEGPIiANsihLUFTvQAlLgj_8C77Faw507nySHYztxTyFd77rlwNV-OsKHt_liwzivTCP1WxxqVe8MZsKpPB14sVH44XTFf-9Eg1B9Y9bW3IdIJxC5ZR080F-jPrgFmxzwg9s1Z3c6GZna_jEz5-CufWvhejl1jFajFRuBRwmaWJxNY6uVt82itjcz9Riu783hlNMM8bBymk72_9XbUYs2Osu6PgwjAZewB9sgTgoDMvShE5-NscaF80g_qQgTkdDDGbo-Hz5TPp5hCC3KrXEH83B1YphlFbMUAtnRXHVv5Ss9lIkwa8kg9JG9-KBc8LOlrD76_sGRqhLUAz_XhRMhDVDbCiz8J84Adzye92yCv4D3GvfI29cJOca1DppZ1wKFh5hhYIjBTrJ3zAuzl4V9its6NCxDGHg_dtm7mIxxwVIED7CXg_ZN3kd0Hi2EKgpesyUK**0b2bcdc94602e07d2932ddcdab701d09d38e7e212071bdaff4617267547f0fe2*5GmDJp7W37qBoivdVyeuXEH2teqZNhOEmJQQUPO7okk"

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
