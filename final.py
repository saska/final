import numpy as np
from scipy.stats import norm

def bs_d1(sigma, time_to_maturity, rf_rate, spot, strike):
    return 1 / (sigma * np.sqrt(time_to_maturity)) \
        * (np.log(spot/strike) + (rf_rate + (sigma**2) / 2) * time_to_maturity)

def bs_d2(sigma, time_to_maturity, rf_rate, spot, strike): 
    return bs_d1(sigma, time_to_maturity, rf_rate, spot, strike) \
        - sigma * np.sqrt(time_to_maturity)

def pv(strike, rate, time_to_maturity):
    return strike * np.exp(-rate * (time_to_maturity))

def european_call_value(sigma, time_to_maturity, rf_rate, spot, strike):
    """Black-Scholes formula for calculating European call option.

    Args: 
        sigma: volatility
        time_to_maturity: T - t where T is exercise date and t is current (pricing) date
        rf_rate: risk-free rate
        spot: Spot price, price of the underlying at t
        strike: Strike price, exercise price of option at T

    sigma, time_to_maturity and rf_rate should be on the same time scale,
    e.g. annualized.
    """
    d1 = bs_d1(sigma, time_to_maturity, rf_rate, spot, strike)
    d2 = bs_d2(sigma, time_to_maturity, rf_rate, spot, strike) # Note: d1 calc made twice
    return norm.cdf(d1) * spot - norm.cdf(d2) * pv(strike, rf_rate, time_to_maturity)

def european_put_value(sigma, time_to_maturity, rf_rate, spot, strike):
    """Put option price with put-call parity.

    Refer to european_call_value.
    """
    call = european_call_value(sigma, time_to_maturity, rf_rate, spot, strike)
    return call - spot + strike * pv(1, rf_rate, time_to_maturity)