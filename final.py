import numpy

def bs_d1(sigma, time_to_maturity, rf_rate, spot, strike):
    return 1 / (sigma * np.sqrt(time_to_maturity)) * (np.log(spot/strike) + (rf_rate + (sigma**2) / 2) * time_to_maturity)

def bs_d2(sigma, time_to_maturity, rf_rate, spot, strike): 
    return bs_d1(sigma, time_to_maturity, rf_rate, spot, strike) - sigma * np.sqrt(time_to_maturity)

def pv(strike, rf_rate, time_to_maturity):
    return strike * np.exp(-rf_rate * (time_to_maturity))

def call_value(sigma, time_to_maturity, rf_rate, spot, strike):
    pass