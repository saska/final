import asyncio
import operator
import concurrent.futures as confu
import collections.OrderedDict
from datetime import date, datetime

class StatusAttributeError(Exception):
    def __init__(self, status, func, *args):
        self.message = f'One or more missing or invalid values for {func} in {status}: {*args}'

def to_datetime(obj, formats=['%Y%m%d']):
    if type(obj) is datetime:
        return obj

    elif type(obj) is date:
        return datetime(obj.year, obj.month, obj.day)

    elif type(obj) is str:
        for f in formats:
            try:
                return datetime.strptime(obj, f)
            except ValueError:
                continue
        raise ValueError(
            f'String {obj} matches none of the formats in {formats}.'
        )
    
class FloorDict(collections.OrderedDict):
    def __get__(self, key):
        try:
            return super().__get__(self, key)
        except KeyError:
            try:
                return super().__get__(
                    self, max(k for k in self if k < key)
                )
            except ValueError:
                raise KeyError(
                    'Key not found and value outside key range'
                )


def price_from_mkt_cap_and_shares(mkt_cap, shares_outstanding):
    return mkt_cap / shares_outstanding

def adj_price(price, cumulative_price_adjust):
    return price / cumulative_price_adjust

def mkt_cap_from_price_and_shares(price, shares_outstanding):
    return price * shares_outstanding

def shares_from_mkt_cap_and_price(mkt_cap, price):
    return mkt_cap / price 

def calc_market_vals(price, mkt_cap, shares_outstanding):
    """Requires (any) two inputs to be non-None"""
    try:
        if price is None:
            _price = mkt_cap / shares_outstanding
        if mkt_cap is None:
            _mkt_cap = price * shares_outstanding
        if shares_outstanding is None:
            _shares_outstanding = mkt_cap / price
    except TypeError as e:
        raise TypeError(
            'calc_market_vals requires at least two inputs to be non-None'
        ) from e

    return _price, _mkt_cap, _shares_outstanding
    

class Status:
    def __init__(self, time, volume=None, price=None,
                 mkt_cap=None, shares_outstanding=None,
                 price_adjust=None, shares_adjust=None):

        self.time = time
        self.volume = volume
        self.price, self.mkt_cap, self.shares_outstanding = calc_market_vals(
            price, mkt_cap, shares_outstanding
        )

        if price_adjust is None:
            self.adj_price = price
        else:
            self.adj_price = price / price_adjust

        if shares_adjust is None:
            self.adj_shares = shares_outstanding
        else:
            self.adj_shares = shares_outstanding * shares_adjust

        self.price_adjust = price_adjust
        self.shares_adjust = shares_adjust

    def __getitem__(self, key):
        return self.attrs[key]

    def __repr__(self):
        return f'{type(self).__name__}({self.time})'    

class Instrument:
    def __init__(self, statuses, name=None, sic_code=None):
        self.statuses = statuses.sort(
            key=operator.attrgetter('time')
        )
        self.name = name

    def __getitem__(self, key):
        return self.statuses[key]

class Index(Instrument):
    def __init__(self, instruments):
        self.instruments = instruments

    @classmethod
    def from_crsp_df_async(cls, df):
        """Create market from CRSP file read into a pandas DataFrame.

        Required columns (as defined by Center for Research in Security Prices):
            PERMNO: Permanent identifier for instrument
            date: date
            SICCD: Standard Industrial Classification code
            VOL: Trading volume
            PRC: price
            SHROUT: Shares outstanding
            CFACPR: Cumulative factor to adjust price
            CFACSHR: Cumulative factor to adjust shares
        """
        df = df.rename(
            mapper={
                'PERMNO': 'name',
                'SICCD': 'sic_code',
                'VOL': 'volume',
                'PRC': 'price',
                'SHROUT': 'shares_outstanding',
                'CFACPR': 'cumulative_price_adjust',
                'CFACSHR': 'cumulative_shares_adjust',
            },
            axis='columns',
        )
        async def create_instrument(name, i):
            statdict = i.to_dict(orient='index')
            statuses = [
                Status(k, v) for k, v in statdict.items()
            ]
            return Instrument(
                    statuses,
                    name=name
                )
            
        
        instruments = []
        async def all_instruments(df):
            for name, i in df.set_index('date').groupby('PERMNO'):
                instruments.append(create_instrument(name, i))
            await asyncio.gather(*instruments)
            
        asyncio.run(all_instruments(df))

        return cls(instruments)

    @classmethod
    def from_crsp_df_sync(cls, df):
        """Create market from CRSP file read into a pandas DataFrame.

        Required columns (as defined by Center for Research in Security Prices):
            PERMNO: Permanent identifier for instrument
            date: date
            SICCD: Standard Industrial Classification code
            VOL: Trading volume
            PRC: price
            SHROUT: Shares outstanding
            CFACPR: Cumulative factor to adjust price
            CFACSHR: Cumulative factor to adjust shares
        """
        
        instruments = []
        for name, i in df.set_index('date').groupby('PERMNO'):
            d = i.to_dict(orient='index')
            statdict = {
                k: d[k] for k in [
                    'date',
                    'PRC', 
                    'VOL',
                    'SHROUT',
                    'CFACPR',
                    'CFACSHR',
                ]
            }
            statuses = [
                Status(k, v) for k, v in statdict.items()
            ]
            instruments.append(Instrument(
                    statuses,
                    name=name
                )
            )

        return cls(instruments)

    @classmethod
    def from_crsp_df_threaded(cls, df):
        """Create market from CRSP file read into a pandas DataFrame.

        Required columns (as defined by Center for Research in Security Prices):
            PERMNO: Permanent identifier for instrument
            date: date
            SICCD: Standard Industrial Classification code
            VOL: Trading volume
            PRC: price
            SHROUT: Shares outstanding
            CFACPR: Cumulative factor to adjust price
            CFACSHR: Cumulative factor to adjust shares
        """
        def create_instrument(name, i):
            statdict = i.to_dict(orient='index')
            statuses = [
                Status(k, v) for k, v in statdict.items()
            ]
            return Instrument(
                        statuses,
                        name=name
                    )

        with confu.ThreadPoolExecutor() as executor:
            instruments = [
                i for i in executor.map(
                    lambda x: create_instrument(*x), 
                    df.set_index('date').groupby('PERMNO')
                )
            ]

        return cls(instruments)