from urllib.request import urlopen
import requests
import re
import os
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


# <a href="archivopub.do?ctrl=MCRST10985ZI342631&amp;id=342631" title="ANÀLISI PDIA PER MUNICIPIS_EAP AMB DADES (XLXS, 2MB)"><img src="tinymce/plugins/tipoarchivos/images/xls.gif" mce_src="tinymce/plugins//tipoarchivos/images/xls.gif" alt="Fulla&nbsp;de&nbsp;Càlcul" title="Fulla&nbsp;de&nbsp;Càlcul" border="0"> ANÀLISI PDIA PER MUNICIPIS_EAP AMB DADES (XLXS, 2MB)</a>


def getFilename_fromCd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]


def goib_xlsx_has_changes(base_directory="../download/gov_xlsx/"):
    url = "https://www.caib.es/sites/covid-19/ca/situacio_sanitaria_covid-19/"
    page = urlopen(url)
    html = page.read().decode("utf-8")
    # print(html)
    reg_exp = '<a href="([^"]*)" title="AN&Agrave;LISI PDIA PER MUNICIPIS_EAP AMB DADES \(XLXS, 2MB\)">'
    match_results = re.search(reg_exp, html)
    document = f"{url}{match_results.group(1)}".replace('&amp;', '&')
    logging.info(f"file url: {document}")
    r = requests.get(document, allow_redirects=True)
    filename = getFilename_fromCd(r.headers.get('content-disposition')).strip('"')
    if not os.path.exists(f"{base_directory}{filename}"):
        open(f"{base_directory}{filename}", 'wb').write(r.content)
        open(f"{base_directory}goib_covid.xlsx", 'wb').write(r.content)
        logging.info("XLSX: Data updated")
        return True

    logging.info("XLSX: No New data")
    return False


if __name__ == "__main__":
    goib_xlsx_has_changes("../download/gov_xlsx/")
