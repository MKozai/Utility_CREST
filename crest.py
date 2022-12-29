import urllib
import pandas as pd
from datetime import datetime as dt

stations = {'Nagoya' : 'Nagoya', 'Hobart' : 'Hobart', 'SaoMartinho' : 'Sao', 'Kuwait' : 'Kuwait', 'Syowa' : 'Syowa'}

def url_check(url):
    try:
        res = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        print(e.code)
        print(e.reason)
        raise Exception('HTTPError!')
    except urllib.error.URLError as e:
        print(e.reason)
        raise Exception('URLError!')
    else:
        res.close()

def load(station, year, corrected=False):
    """
    Read archive data from Shinshu Univ. CREST website.

    Parameters
    ----------
    station: str
        Station name.
    year: int
        Year of archive data file.
    corrected: bool
        Corrected for atmospheric pressure or not, default False.

    Returns
    -------
    DataFrame
        Read data table.
    """

    str_corrected = ['Uncorrect','raw']
    if corrected: str_corrected = ['Correct','copre']
    url = 'http://cosray.shinshu-u.ac.jp/crest/DB/Public/Archives/GMDN/' + str_corrected[0] + '/' + station + '/' + stations[station] + str(year) + '_1hour_' + str_corrected[1] + '+error.txt'
    print('Read remote data: ' + url)
    try:
        url_check(url)
    except Exception as e:
        print(e)
        print('Data download Failed!')
        return None

    with urllib.request.urlopen(url) as f:
        lines = f.read().decode('utf-8').split('\n')
        i_header = [i for i, s in enumerate(lines) if s.startswith('year month day ')]
        header = lines[i_header[0]].split()

        print('Parse to DataFrame')
        mat = [list(map(
            lambda s: float(s) if '.' in s else int(s),
            line.split()
            ))
            for line in lines[i_header[0]+2:len(lines)-1]]
        data = pd.DataFrame(mat, columns=header)

        print('Create datetime data (TIME column)')
        data['TIME'] = list(map(
            lambda year,month,day,hour,minute: dt(year,month,day,hour,minute),
            data['year'],data['month'],data['day'],data['hour'],[30]*len(data)
            ))

    print('Finished!')
    return data
