import datetime

#epoch = datetime.datetime(2016, 4, 22)


def from_sas_date(sas_date: float, epoch: datetime):
    return epoch + datetime.timedelta(seconds=sas_date)


def get_dates(arrival: float, departure: float, epoch: datetime, from_location: str, to_location: str):
    arr = from_sas_date(arrival, epoch)
    dep = from_sas_date(departure, epoch)
    print(f"from {from_location} to {to_location} : timedelta: {arrival-departure},  departs: {dep}:   arrives: {arr}")


"""
index: 2
cicid: 15
i94yr: 2016
i94mon: 4
i94cit: 101        # ALBANIA
i94res: 101        # ALBANIA
i94port: WAS       # WASHINGTON DC (
arrdate: 20545
i94mode: 1
i94addr: MI        # MICHIGAN
depdate: 20691     # 20691 - 20545 =  146 .... 
i94bir: 55
i94visa: 2
count: 1
dtadfile: '20160401'
visapost: null
occup: null
entdepa: T
entdepd: O
entdepu: null
matflag: M
biryear: 1961
dtaddto: 09302016
gender: M
insnum: null
airline: OS
admnum: 666643185
fltno: '93'
visatype: B2
ctid: '(0,3)'
"""
if __name__ == "__main__":
    get_dates(arrival=20545, departure=20691, epoch= datetime.datetime(2016,9,30), from_location="ALBANIA", to_location="WASHINGTON DC, HI")