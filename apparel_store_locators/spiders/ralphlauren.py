import base64
import json
import urllib.parse
from datetime import datetime
import scrapy
from scrapy.cmdline import execute
import pandas as pd

from apparel_store_locators.items import ApparelStoreLocatorsItem


class RalphlaurenSpider(scrapy.Spider):
    name = "ralphlauren"

    def __init__(self):
        super().__init__()
        self.headers = None
        self.cookies = None

    def start_requests(self):
        self.cookies = {
            'dwanonymous_55b6a3b329e729876c1d594e39f4ac4e': 'bcaJKG1N9M9j9N49NbakHQyT3d',
            '_pxhd': 'SRq7qmPU64YWFRXhm7lyeOzOf6wJsl5A5AJOFRSYU5fJq6B22V3VuMy9vklqnUgLJGTZvmw/7oy17I4/ptHcXQ==:/5wGX-n4oOFIWCrHfjLGH60zPWbJ1Us/p0/FbC55dRWLtPRKGHahi9NdUAvSyiZRluJ5bK4AJHEonDlXLcOJTURXdI6nC5cDcqMSXpT62Jo=',
            '_pxvid': 'a503d4e1-abc7-11ef-9d67-b020ae644c30',
            '__pxvid': 'ab88d2ac-abc7-11ef-abd1-0242ac120003',
            'pzcookie': '"{\\"pz_id\\":\\"\\",\\"EP_RID\\":\\"\\"}"',
            'modalLoad': '1',
            'headerDefault': '1',
            '_gcl_au': '1.1.1501900772.1732605974',
            '_scid': '_0AFGvSd3uogLas21C22KfVVwZVoKDZA',
            '__rtbh.uid': '%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22unknown%22%2C%22expiryDate%22%3A%222025-11-26T07%3A26%3A15.582Z%22%7D',
            '_fbp': 'fb.1.1732605976156.796795269960798753',
            '_gid': 'GA1.2.109582485.1732605976',
            '_pin_unauth': 'dWlkPU5UQTVaRGMwTXpVdFlXRTNZUzAwWVRGaExUbGhNVFl0TmpJek9ESTNPV1prWmpZeQ',
            'kampyle_userid': '8d98-8e82-8bdc-38a2-77cf-a90b-9600-2dbd',
            '__cq_uuid': 'ab2zY44qzcHLu5qPiRHiz1m2SI',
            '__cq_seg': '0~0.00!1~0.00!2~0.00!3~0.00!4~0.00!5~0.00!6~0.00!7~0.00!8~0.00!9~0.00',
            '_ScCbts': '%5B%5D',
            'crl8.fpcuid': '62a793ad-50dc-4b2f-a644-59d8db9de934',
            '_sctr': '1%7C1732559400000',
            '_tt_enable_cookie': '1',
            '_ttp': '3wRUDm4k4m7PnR9LeLraIKbQxKa.tt.1',
            '__exponea_etc__': '0da3bc79-3655-44b7-bf71-94436e0d10f0',
            'afUserId': '11d0b747-2305-468e-8789-a30d1319da42-p',
            'AF_SYNC': '1732605983690',
            '_cs_c': '0',
            'kndctr_F18502BE5329FB670A490D4C_AdobeOrg_identity': 'CiYwMDU0MDI1MzU1ODc3ODI5MDA4MDk3OTMxNjEyNTEzNjg0NTIwNVISCLig9Lq2MhABGAEqA1ZBNjAA8AG4oPS6tjI%3D',
            'AMCV_F18502BE5329FB670A490D4C%40AdobeOrg': 'MCMID|00540253558778290080979316125136845205',
            'mt.v': '5.1969749546.1732605942701',
            'dwac_102c95db27e6f188d36d6303ba': 'CyN6EoBYzIo0ApmW-Z1TpmaEuWkVu1Vo04k%3D|dw-only|||USD|false|US%2FEastern|true',
            'cqcid': 'bcaJKG1N9M9j9N49NbakHQyT3d',
            'cquid': '||',
            'sid': 'CyN6EoBYzIo0ApmW-Z1TpmaEuWkVu1Vo04k',
            '__cq_dnt': '0',
            'dw_dnt': '0',
            'dwsid': 'asVhQas_O4Euj1a2WcOG2gdiojo-3KJjyQQKj8fJ8oJaHfNERysLXfqm4_YkOaUau-NHufXuUAdokMHFYCcv5A==',
            'pxcts': 'b94a3aaf-abd4-11ef-8d2f-45fd147e3837',
            'dw': '1',
            'dw_cookies_accepted': '1',
            'rmStore': 'atm:mop',
            'kampyleUserSession': '1732611592794',
            'kampyleUserSessionsCount': '3',
            'kampyleUserPercentile': '61.0592661717265',
            '_cs_cvars': '%7B%7D',
            'pageNameDuplicate': 'about:storelocator',
            '_ga': 'GA1.1.1521733503.1732605973',
            'OptanonConsent': 'isGpcEnabled=0&datestamp=Tue+Nov+26+2024+17%3A34%3A36+GMT%2B0530+(India+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=1%3A1%2C3%3A1%2CBG71%3A1%2C2%3A1%2C4%3A1&AwaitingReconsent=false',
            '_ga_MHJQ8DE280': 'GS1.1.1732622678.4.0.1732622678.60.0.0',
            '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22cvltJkFskBWj7UneKKZA%22%2C%22expiryDate%22%3A%222025-11-26T12%3A04%3A38.523Z%22%7D',
            '_scid_r': 'EMAFGvSd3uogLas21C22KfVVwZVoKDZAnxXDYA',
            '_uetsid': 'b8ea5760abc711ef993581de69fdb9da',
            '_uetvid': 'b8eabc00abc711ef8ac2d7f55924234c',
            'kampyleSessionPageCounter': '3',
            '_br_uid_2': 'uid%3D9914551262435%3Av%3D17.0%3Ats%3D1732605981171%3Ahc%3D10',
            '_cs_id': '5e8af6ec-fa3d-a1db-9778-132a615c9496.1732605983.2.1732622698.1732616354.1724195521.1766769983996.1',
            'forterToken': 'fe797c48d10b4616921dc9c1bb5e200b_1732622675594__UDF43-m4_17ck_',
            '_px2': 'eyJ1IjoiOWE0NGExZDAtYWJlZS0xMWVmLTg2MjYtOTVlMDA4MDMxOTNhIiwidiI6ImE1MDNkNGUxLWFiYzctMTFlZi05ZDY3LWIwMjBhZTY0NGMzMCIsInQiOjE1Mjk5NzEyMDAwMDAsImgiOiJlMWU3NjIyY2Y4MTkyYjM5MDFlY2Y2ODA2YWUzZjUyY2U3NjI1OTQ1NmM5MDlkZmYyNzgwNzc1ZGZjMzdlNjAzIn0=',
            's_ips': '486.6666717529297',
            's_tp': '1384',
            's_ppv': 'en_us%253Astorelocator%253Aresults%2520page%2C35%2C35%2C486.6666717529297%2C1%2C4',
            'RT': '"z=1&dm=ralphlauren.com&si=0be8a205-8e13-40e9-971e-0c2ffc806bcf&ss=m3y86vuh&sl=0&tt=0&bcn=%2F%2F684d0d4a.akstat.io%2F"',
            '_cs_s': '3.0.1.9.1732628345867',
        }

        self.headers = {
            # 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            # 'accept-language': 'en-US,en;q=0.9',
            # 'cache-control': 'no-cache',
            # # 'cookie': 'dwanonymous_55b6a3b329e729876c1d594e39f4ac4e=bcaJKG1N9M9j9N49NbakHQyT3d; _pxhd=SRq7qmPU64YWFRXhm7lyeOzOf6wJsl5A5AJOFRSYU5fJq6B22V3VuMy9vklqnUgLJGTZvmw/7oy17I4/ptHcXQ==:/5wGX-n4oOFIWCrHfjLGH60zPWbJ1Us/p0/FbC55dRWLtPRKGHahi9NdUAvSyiZRluJ5bK4AJHEonDlXLcOJTURXdI6nC5cDcqMSXpT62Jo=; _pxvid=a503d4e1-abc7-11ef-9d67-b020ae644c30; __pxvid=ab88d2ac-abc7-11ef-abd1-0242ac120003; pzcookie="{\\"pz_id\\":\\"\\",\\"EP_RID\\":\\"\\"}"; modalLoad=1; headerDefault=1; _gcl_au=1.1.1501900772.1732605974; _scid=_0AFGvSd3uogLas21C22KfVVwZVoKDZA; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22unknown%22%2C%22expiryDate%22%3A%222025-11-26T07%3A26%3A15.582Z%22%7D; _fbp=fb.1.1732605976156.796795269960798753; _gid=GA1.2.109582485.1732605976; _pin_unauth=dWlkPU5UQTVaRGMwTXpVdFlXRTNZUzAwWVRGaExUbGhNVFl0TmpJek9ESTNPV1prWmpZeQ; kampyle_userid=8d98-8e82-8bdc-38a2-77cf-a90b-9600-2dbd; __cq_uuid=ab2zY44qzcHLu5qPiRHiz1m2SI; __cq_seg=0~0.00!1~0.00!2~0.00!3~0.00!4~0.00!5~0.00!6~0.00!7~0.00!8~0.00!9~0.00; _ScCbts=%5B%5D; crl8.fpcuid=62a793ad-50dc-4b2f-a644-59d8db9de934; _sctr=1%7C1732559400000; _tt_enable_cookie=1; _ttp=3wRUDm4k4m7PnR9LeLraIKbQxKa.tt.1; __exponea_etc__=0da3bc79-3655-44b7-bf71-94436e0d10f0; afUserId=11d0b747-2305-468e-8789-a30d1319da42-p; AF_SYNC=1732605983690; _cs_c=0; kndctr_F18502BE5329FB670A490D4C_AdobeOrg_identity=CiYwMDU0MDI1MzU1ODc3ODI5MDA4MDk3OTMxNjEyNTEzNjg0NTIwNVISCLig9Lq2MhABGAEqA1ZBNjAA8AG4oPS6tjI%3D; AMCV_F18502BE5329FB670A490D4C%40AdobeOrg=MCMID|00540253558778290080979316125136845205; mt.v=5.1969749546.1732605942701; dwac_102c95db27e6f188d36d6303ba=CyN6EoBYzIo0ApmW-Z1TpmaEuWkVu1Vo04k%3D|dw-only|||USD|false|US%2FEastern|true; cqcid=bcaJKG1N9M9j9N49NbakHQyT3d; cquid=||; sid=CyN6EoBYzIo0ApmW-Z1TpmaEuWkVu1Vo04k; __cq_dnt=0; dw_dnt=0; dwsid=asVhQas_O4Euj1a2WcOG2gdiojo-3KJjyQQKj8fJ8oJaHfNERysLXfqm4_YkOaUau-NHufXuUAdokMHFYCcv5A==; pxcts=b94a3aaf-abd4-11ef-8d2f-45fd147e3837; dw=1; dw_cookies_accepted=1; rmStore=atm:mop; kampyleUserSession=1732611592794; kampyleUserSessionsCount=3; kampyleUserPercentile=61.0592661717265; _cs_cvars=%7B%7D; pageNameDuplicate=about:storelocator; _ga=GA1.1.1521733503.1732605973; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Nov+26+2024+17%3A34%3A36+GMT%2B0530+(India+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=1%3A1%2C3%3A1%2CBG71%3A1%2C2%3A1%2C4%3A1&AwaitingReconsent=false; _ga_MHJQ8DE280=GS1.1.1732622678.4.0.1732622678.60.0.0; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22cvltJkFskBWj7UneKKZA%22%2C%22expiryDate%22%3A%222025-11-26T12%3A04%3A38.523Z%22%7D; _scid_r=EMAFGvSd3uogLas21C22KfVVwZVoKDZAnxXDYA; _uetsid=b8ea5760abc711ef993581de69fdb9da; _uetvid=b8eabc00abc711ef8ac2d7f55924234c; kampyleSessionPageCounter=3; _br_uid_2=uid%3D9914551262435%3Av%3D17.0%3Ats%3D1732605981171%3Ahc%3D10; _cs_id=5e8af6ec-fa3d-a1db-9778-132a615c9496.1732605983.2.1732622698.1732616354.1724195521.1766769983996.1; forterToken=fe797c48d10b4616921dc9c1bb5e200b_1732622675594__UDF43-m4_17ck_; _px2=eyJ1IjoiOWE0NGExZDAtYWJlZS0xMWVmLTg2MjYtOTVlMDA4MDMxOTNhIiwidiI6ImE1MDNkNGUxLWFiYzctMTFlZi05ZDY3LWIwMjBhZTY0NGMzMCIsInQiOjE1Mjk5NzEyMDAwMDAsImgiOiJlMWU3NjIyY2Y4MTkyYjM5MDFlY2Y2ODA2YWUzZjUyY2U3NjI1OTQ1NmM5MDlkZmYyNzgwNzc1ZGZjMzdlNjAzIn0=; s_ips=486.6666717529297; s_tp=1384; s_ppv=en_us%253Astorelocator%253Aresults%2520page%2C35%2C35%2C486.6666717529297%2C1%2C4; RT="z=1&dm=ralphlauren.com&si=0be8a205-8e13-40e9-971e-0c2ffc806bcf&ss=m3y86vuh&sl=0&tt=0&bcn=%2F%2F684d0d4a.akstat.io%2F"; _cs_s=3.0.1.9.1732628345867',
            # 'pragma': 'no-cache',
            # 'priority': 'u=0, i',
            # 'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            # 'sec-ch-ua-mobile': '?0',
            # 'sec-ch-ua-platform': '"Windows"',
            # 'sec-fetch-dest': 'document',
            # 'sec-fetch-mode': 'navigate',
            # 'sec-fetch-site': 'none',
            # 'sec-fetch-user': '?1',
            # 'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        yield scrapy.Request(
            url="https://www.ralphlauren.com/findstores?dwfrm_storelocator_country=US&dwfrm_storelocator_findbycountry=Search&findByValue=CountrySearch",
            headers=self.headers,
            # cookies=self.cookies,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        raw_data = response.xpath('//div[contains(@class,"storeJSON")]/@data-storejson').get()
        json_data = json.loads(raw_data)
        for data in json_data:
            item = ApparelStoreLocatorsItem()
            item['store_id'] = data['id']
            item['name'] = base64.b64decode(data['name']).decode("utf-8")
            item['latitude'] = data['latitude']
            item['longitude'] = data['longitude']
            item['city'] = data['city']
            item['state'] = data['stateCode']
            item['zip_code'] = data['postalCode']
            item['county'] = ''
            item['phone'] = data['phone'].replace('.', '') if data['phone'] else ''
            item['provider'] = "Ralph Lauren"
            item['category'] = "Apparel"
            item['updated_date'] = datetime.today().strftime('%d-%m-%Y')
            item['country'] = 'USA'

            if data['city'] is None:
                continue
            cookies = {
                'dwanonymous_55b6a3b329e729876c1d594e39f4ac4e': 'bcaJKG1N9M9j9N49NbakHQyT3d',
                '_pxvid': 'a503d4e1-abc7-11ef-9d67-b020ae644c30',
                '__pxvid': 'ab88d2ac-abc7-11ef-abd1-0242ac120003',
                'pzcookie': '"{\\"pz_id\\":\\"\\",\\"EP_RID\\":\\"\\"}"',
                'modalLoad': '1',
                'headerDefault': '1',
                '_gcl_au': '1.1.1501900772.1732605974',
                '_scid': '_0AFGvSd3uogLas21C22KfVVwZVoKDZA',
                '__rtbh.uid': '%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22unknown%22%2C%22expiryDate%22%3A%222025-11-26T07%3A26%3A15.582Z%22%7D',
                '_fbp': 'fb.1.1732605976156.796795269960798753',
                '_gid': 'GA1.2.109582485.1732605976',
                '_pin_unauth': 'dWlkPU5UQTVaRGMwTXpVdFlXRTNZUzAwWVRGaExUbGhNVFl0TmpJek9ESTNPV1prWmpZeQ',
                'kampyle_userid': '8d98-8e82-8bdc-38a2-77cf-a90b-9600-2dbd',
                '_ScCbts': '%5B%5D',
                'crl8.fpcuid': '62a793ad-50dc-4b2f-a644-59d8db9de934',
                '_sctr': '1%7C1732559400000',
                '_tt_enable_cookie': '1',
                '_ttp': '3wRUDm4k4m7PnR9LeLraIKbQxKa.tt.1',
                '__exponea_etc__': '0da3bc79-3655-44b7-bf71-94436e0d10f0',
                'afUserId': '11d0b747-2305-468e-8789-a30d1319da42-p',
                'AF_SYNC': '1732605983690',
                '_cs_c': '0',
                'kndctr_F18502BE5329FB670A490D4C_AdobeOrg_identity': 'CiYwMDU0MDI1MzU1ODc3ODI5MDA4MDk3OTMxNjEyNTEzNjg0NTIwNVISCLig9Lq2MhABGAEqA1ZBNjAA8AG4oPS6tjI%3D',
                'AMCV_F18502BE5329FB670A490D4C%40AdobeOrg': 'MCMID|00540253558778290080979316125136845205',
                'mt.v': '5.1969749546.1732605942701',
                'rmStore': 'atm:mop',
                'pxcts': '28622fd6-ac73-11ef-9204-8beafae0388e',
                'sid': 'jLwtD8J5YaAPIt5Iz3SVaaxIb1E2qZ9tAAs',
                'dwsid': 'nxRYpJ_tu6YgGrRVorRTbb_gGvprPUVrLzMwSnSyZR49nFn9nbX_W7SUS1k4pVtwLTPlS9Nmijs_85jh2KaNKw==',
                'dw': '1',
                'dw_cookies_accepted': '1',
                '_cs_cvars': '%7B%7D',
                'kampylePageLoadedTimestamp': '1732684916156',
                '_ga': 'GA1.1.1521733503.1732605973',
                'pageNameDuplicate': 'about:storelocator',
                'dwac_102c95db27e6f188d36d6303ba': 'jLwtD8J5YaAPIt5Iz3SVaaxIb1E2qZ9tAAs%3D|dw-only|||USD|false|US%2FEastern|true',
                'cqcid': 'bcaJKG1N9M9j9N49NbakHQyT3d',
                'cquid': '||',
                'kampyleUserSession': '1732686086903',
                'kampyleUserSessionsCount': '16',
                'kampyleUserPercentile': '29.413157235565635',
                '_pxhd': 'Ou8Lu451dBkvb7rSNz/ejY8mHFp5SkcTetCRlKFfnzn/nbX67gMkDXVIs8-pojqUyjG8f1cqyiPLMfaUkdm09A==:UxQKB80-45ZTn0KyMSAeQ0J/WBbuxtRbx9HdVmCN0XLWQMvN/oZ7fm5yukiObnud5kbjZUqO6GlCZaJX6NPSi4t6flP02MO5Pcysqwv7i64=',
                '_px2': 'eyJ1IjoiMzZjNDM5MTAtYWM4Mi0xMWVmLWFhMzEtMmZjZjRjMDY3Y2QxIiwidiI6ImE1MDNkNGUxLWFiYzctMTFlZi05ZDY3LWIwMjBhZTY0NGMzMCIsInQiOjE1NjE1MDcyMDAwMDAsImgiOiJkNjE2MmExYzBjMDA4NGYyNDIyMmUzMGJkYzc1OGIwZDM0ZDhkYTZlNGNhYzY5NjM0NjRlMTc2MmYxMzA1OTkzIn0=',
                '__cq_dnt': '0',
                'dw_dnt': '0',
                'OptanonConsent': 'isGpcEnabled=0&datestamp=Wed+Nov+27+2024+13%3A21%3A18+GMT%2B0530+(India+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=1%3A1%2C3%3A1%2CBG71%3A1%2C2%3A1%2C4%3A1&AwaitingReconsent=false',
                '_cs_mk_aa': '0.2957183522759308_1732693879964',
                's_tp': '1787',
                'RT': '"z=1&dm=ralphlauren.com&si=02bde269-211a-4140-8c90-f0c0b5389a01&ss=m3zl6yjr&sl=1&tt=70r&bcn=%2F%2F684d0d43.akstat.io%2F&ld=7s2"',
                '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22cvltJkFskBWj7UneKKZA%22%2C%22expiryDate%22%3A%222025-11-27T07%3A51%3A20.918Z%22%7D',
                '_scid_r': 'FcAFGvSd3uogLas21C22KfVVwZVoKDZAnxXDdA',
                's_ips': '621',
                '_uetsid': 'b8ea5760abc711ef993581de69fdb9da',
                '_uetvid': 'b8eabc00abc711ef8ac2d7f55924234c',
                '_ga_MHJQ8DE280': 'GS1.1.1732693885.8.0.1732693885.60.0.0',
                '__cq_uuid': 'ab2zY44qzcHLu5qPiRHiz1m2SI',
                '__cq_seg': '0~0.00!1~0.00!2~0.00!3~0.00!4~0.00!5~0.00!6~0.00!7~0.00!8~0.00!9~0.00',
                's_ppv': 'about%253Astorelocator%2C55%2C35%2C974%2C1%2C3',
                'kampyleSessionPageCounter': '2',
                '_br_uid_2': 'uid%3D9914551262435%3Av%3D17.0%3Ats%3D1732605981171%3Ahc%3D30',
                'forterToken': 'fe797c48d10b4616921dc9c1bb5e200b_1732693878007__UDF43-mnf-a4_17ck_',
                '_cs_id': '5e8af6ec-fa3d-a1db-9778-132a615c9496.1732605983.4.1732693890.1732693890.1724195521.1766769983996.1',
                '__exponea_time2__': '-14.882128953933716',
                '_cs_s': '1.5.1.9.1732695720998',
            }

            headers = {
                # 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                # 'accept-language': 'en-US,en;q=0.9',
                # 'cache-control': 'no-cache',
                # # 'cookie': 'dwanonymous_55b6a3b329e729876c1d594e39f4ac4e=bcaJKG1N9M9j9N49NbakHQyT3d; _pxvid=a503d4e1-abc7-11ef-9d67-b020ae644c30; __pxvid=ab88d2ac-abc7-11ef-abd1-0242ac120003; pzcookie="{\\"pz_id\\":\\"\\",\\"EP_RID\\":\\"\\"}"; modalLoad=1; headerDefault=1; _gcl_au=1.1.1501900772.1732605974; _scid=_0AFGvSd3uogLas21C22KfVVwZVoKDZA; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22unknown%22%2C%22expiryDate%22%3A%222025-11-26T07%3A26%3A15.582Z%22%7D; _fbp=fb.1.1732605976156.796795269960798753; _gid=GA1.2.109582485.1732605976; _pin_unauth=dWlkPU5UQTVaRGMwTXpVdFlXRTNZUzAwWVRGaExUbGhNVFl0TmpJek9ESTNPV1prWmpZeQ; kampyle_userid=8d98-8e82-8bdc-38a2-77cf-a90b-9600-2dbd; __cq_uuid=ab2zY44qzcHLu5qPiRHiz1m2SI; __cq_seg=0~0.00!1~0.00!2~0.00!3~0.00!4~0.00!5~0.00!6~0.00!7~0.00!8~0.00!9~0.00; _ScCbts=%5B%5D; crl8.fpcuid=62a793ad-50dc-4b2f-a644-59d8db9de934; _sctr=1%7C1732559400000; _tt_enable_cookie=1; _ttp=3wRUDm4k4m7PnR9LeLraIKbQxKa.tt.1; __exponea_etc__=0da3bc79-3655-44b7-bf71-94436e0d10f0; afUserId=11d0b747-2305-468e-8789-a30d1319da42-p; AF_SYNC=1732605983690; _cs_c=0; kndctr_F18502BE5329FB670A490D4C_AdobeOrg_identity=CiYwMDU0MDI1MzU1ODc3ODI5MDA4MDk3OTMxNjEyNTEzNjg0NTIwNVISCLig9Lq2MhABGAEqA1ZBNjAA8AG4oPS6tjI%3D; AMCV_F18502BE5329FB670A490D4C%40AdobeOrg=MCMID|00540253558778290080979316125136845205; mt.v=5.1969749546.1732605942701; rmStore=atm:mop; _ga=GA1.1.1521733503.1732605973; kampyleUserSession=1732627473249; kampyleUserSessionsCount=5; kampyleUserPercentile=20.837044955846352; _pxhd=vKZL-cP5VjOG6mvfP0ZMRF01vVBtoGx49pco5j27e6vJ/wxE8PfLvRVSBJNQMdq7d7izPPHUMJzCPT7cWwBteg==:ENR-4ZGj2W0td2hZUxU3hoR/PfiCIzauWlHo/woU2KvI0nQAV6Bh8TSx5wrEi4EzR7SAIWYJRODwOKx7sxbDZnBZCd9/VxN9bK/gjf-1ysE=; pxcts=28622fd6-ac73-11ef-9204-8beafae0388e; _px2=eyJ1IjoiMjZiMjg2YzItYWM3My0xMWVmLTk5NjAtNGQyZWYyZjIwNjA1IiwidiI6ImE1MDNkNGUxLWFiYzctMTFlZi05ZDY3LWIwMjBhZTY0NGMzMCIsInQiOjE1NjE1MDcyMDAwMDAsImgiOiI2YjQ4Nzk0NzM1Nzk3ZDNlMTljMTQ2YmRiNTA1ZGI5YTJjZDZlNmViYThjZGJiN2VkNTI0YjI0YzY2Y2QxZWRmIn0=; dwac_102c95db27e6f188d36d6303ba=jLwtD8J5YaAPIt5Iz3SVaaxIb1E2qZ9tAAs%3D|dw-only|||USD|false|US%2FEastern|true; cqcid=bcaJKG1N9M9j9N49NbakHQyT3d; cquid=||; sid=jLwtD8J5YaAPIt5Iz3SVaaxIb1E2qZ9tAAs; __cq_dnt=0; dw_dnt=0; dwsid=nxRYpJ_tu6YgGrRVorRTbb_gGvprPUVrLzMwSnSyZR49nFn9nbX_W7SUS1k4pVtwLTPlS9Nmijs_85jh2KaNKw==; dw=1; dw_cookies_accepted=1; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Nov+27+2024+09%3A24%3A12+GMT%2B0530+(India+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=1%3A1%2C3%3A1%2CBG71%3A1%2C2%3A1%2C4%3A1&AwaitingReconsent=false; _cs_mk_aa=0.8243234624155331_1732679653252; pageNameDuplicate=about:storelocator; s_tp=2518; _ga_MHJQ8DE280=GS1.1.1732679653.6.0.1732679653.60.0.0; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22cvltJkFskBWj7UneKKZA%22%2C%22expiryDate%22%3A%222025-11-27T03%3A54%3A13.900Z%22%7D; s_ips=853; s_ppv=about%253Astorelocator%2C34%2C34%2C853%2C1%2C4; kampyleSessionPageCounter=2; _uetsid=b8ea5760abc711ef993581de69fdb9da; _uetvid=b8eabc00abc711ef8ac2d7f55924234c; _scid_r=FMAFGvSd3uogLas21C22KfVVwZVoKDZAnxXDZA; _br_uid_2=uid%3D9914551262435%3Av%3D17.0%3Ats%3D1732605981171%3Ahc%3D14; _cs_cvars=%7B%7D; _cs_id=5e8af6ec-fa3d-a1db-9778-132a615c9496.1732605983.3.1732679661.1732679661.1724195521.1766769983996.1; __exponea_time2__=-15.523144721984863; RT="z=1&dm=ralphlauren.com&si=0be8a205-8e13-40e9-971e-0c2ffc806bcf&ss=m3zcq9ht&sl=0&tt=0&bcn=%2F%2F684d0d43.akstat.io%2F&ld=v7qud"; _cs_s=1.0.1.9.1732681463926; forterToken=fe797c48d10b4616921dc9c1bb5e200b_1732679651797__UDF43-m4_17ck_',
                # 'pragma': 'no-cache',
                # 'priority': 'u=0, i',
                # 'referer': 'https://www.ralphlauren.com/locations/US/Charlotte/451',
                # 'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                # 'sec-ch-ua-mobile': '?0',
                # 'sec-ch-ua-platform': '"Windows"',
                # 'sec-fetch-dest': 'document',
                # 'sec-fetch-mode': 'navigate',
                # 'sec-fetch-site': 'same-origin',
                # 'sec-fetch-user': '?1',
                # 'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            }

            yield scrapy.Request(
                url=f"https://www.ralphlauren.com/locations/US/{urllib.parse.quote(data['city'])}/{data['id']}",
                headers=headers,
                cookies=cookies,
                callback=self.parse_data,
                cb_kwargs={'item': item}
            )

            # params = {
            #     "hl": "en",
            #     "f": "q",
            #     "q": ", Suite B14, Tannersville, 18372, PA, US"
            # }
            # item['direction_url'] = 'http://maps.google.com/maps?hl=en&f=q&q=1000%20Premium%20Outlets%20Drive%20,%20Suite%20B14,%20Tannersville,%2018372,%20PA,%20US'
            # item

    def parse_data(self, response, **kwargs):
        # raw_data = response.xpath('//script[@type="application/ld+json" and contains(text(),\'"@type":"Store"\')]/text()').get()
        # if not raw_data:
        #     pd.read_html()
        # json_data = json.loads(raw_data)
        item = kwargs['item']
        item['url'] = response.url
        # address_data = json_data['address']
        # item['street'] = address_data['streetAddress']
        # item['open_hours'] = ' | '.join(self.convert_to_24_hour_format(json_data['openingHours']))
        item['direction_url'] = response.xpath('//a[@class="google-map"]/@href').get('')
        # item['status'] = ''
        # yield item
        # Extract rows from the store hours table using XPath
        item['street'] = ' '.join(self.extract_street(response, item['city'], item['state'], item['zip_code']).split())
        item['open_hours'] = self.extract_opening_hours(response)
        item['status'] = self.extract_status(response)
        yield item

    def extract_street(self, response, city, state, zip_code):
        street = response.xpath('//div[contains(@class, "store-address")]/span/text()').getall()
        return ' '.strip().join(street).replace('United States', '').replace(city, '').replace(state, '').replace(zip_code,'').replace(',','').strip()

    def extract_status(self, response):
        status = response.xpath('//div[@class="store-status"]/text()').get()
        if status:
            return 'OPEN' if 'open' in status.lower() else 'CLOSE'
        else:
            return ''

    def extract_opening_hours(self, response):
        # Define all days of the week
        all_days = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]

        # Try to extract rows in the original structure
        rows = response.xpath('//table[@class="store-hours"]//tr[@class="store-hourrow"]')
        hours_dict = {}

        if rows:  # Handle the original structure
            for row in rows:
                day = row.xpath('td[@class="store-hours-day"]/text()').get().strip()
                if day == 'Closed':
                    return ''
                time_range = row.xpath('td[@class="store-hours-open"]/text()').get()
                if not time_range:
                    combined_text = response.xpath(
                        '//table[@class="store-hours"]//td[@class="store-hours-day"]/text()').get()
                    if combined_text:
                        for line in combined_text.split('<br>'):  # Split each line by <br>
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                day, time_range = line.split(':', 1)
                                day = day.strip()
                                time_range = time_range.strip()

                                if ' - ' in time_range:
                                    start, end = time_range.split(' - ')
                                    start_24hr = self.convert_to_24hr(start.strip())
                                    end_24hr = self.convert_to_24hr(end.strip())
                                    hours_dict[day] = f"{start_24hr}-{end_24hr}"
                                else:
                                    hours_dict[day] = "Closed"
                            except ValueError:
                                self.log(f"Invalid format in line: {line}")
                                hours_dict[day] = "Closed"
                else:
                    time_range = time_range.strip()
                    if ' - ' in time_range:  # Valid time range
                        try:
                            start, end = time_range.split(' - ')
                            start_24hr = self.convert_to_24hr(start.strip())
                            end_24hr = self.convert_to_24hr(end.strip())
                            hours_dict[day] = f"{start_24hr}-{end_24hr}"
                        except Exception as e:
                            self.log(f"Error processing time range '{time_range}': {e}")
                            hours_dict[day] = "Closed"
                    else:
                        hours_dict[day] = "Closed"


        # Ensure all days are accounted for
        formatted_hours = []
        for day in all_days:
            if day in hours_dict:
                formatted_hours.append(f"{day}: {hours_dict[day]}")
            else:
                formatted_hours.append(f"{day} Closed")

        # Join all formatted hours with " | " separator
        return " | ".join(formatted_hours)

    def convert_to_24hr(self, time_str):
        """
        Converts time from AM/PM format to 24-hour format.
        Example: '11:00 AM' -> '11:00', '8:00 PM' -> '20:00'
        """
        from datetime import datetime
        time_str = time_str.strip()
        time_obj = datetime.strptime(time_str, '%I:%M %p')
        return time_obj.strftime('%H:%M')


if __name__ == '__main__':
    execute(f'scrapy crawl {RalphlaurenSpider.name}'.split())
