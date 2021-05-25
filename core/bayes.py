import numpy as np
from math import lgamma
from scipy.stats import beta as beta_distr
from numba import njit


class BayesTest():
    """
    Класс для Байессовского АБ теста. Изначально он делался для показов и конверсий
    """

    def __init__(self, impressions_ctrl, conversions_ctrl, impressions_test, conversions_test):
        self.impressions_ctrl = impressions_ctrl
        self.conversions_ctrl = conversions_ctrl
        self.impressions_test = impressions_test
        self.conversions_test = conversions_test

    # @njit
    def h(self, a, b, c, d):
        num = lgamma(a + c) + lgamma(b + d) + lgamma(a + b) + lgamma(c + d)
        den = (lgamma(a) + lgamma(b) + lgamma(c) + lgamma(d)
               + lgamma(a + b + c + d)
               )
        return np.exp(num - den)

    # @njit
    def g0(self, a, b, c):
        return np.exp(lgamma(a + b) + lgamma(a + c) -
                      (lgamma(a + b + c) + lgamma(a)))

    # @njit
    def calc(self, a, b, c, d):
        while d > 1:
            d -= 1
            yield self.h(a, b, c, d) / d

    # @njit
    def g(self, a, b, c, d):
        return self.g0(a, b, c) + sum(self.calc(a, b, c, d))

    # @njit
    def calc_prob(self, beta1, beta2):
        return self.g(beta1.args[0], beta1.args[1], beta2.args[0], beta2.args[1])

    @staticmethod
    def beta(impressions, conversions):
        a, b = conversions+1, impressions-conversions+1

        return beta_distr(a, b)

    @staticmethod
    def lift(before, after):
        return (after.mean() - before.mean()) / after.mean()

    def bayes_prob(self, verbose=1):
        beta_c = self.beta(self.impressions_ctrl, self.conversions_ctrl)
        beta_t = self.beta(self.impressions_test, self.conversions_test)

        print(beta_c.mean())
        print(beta_t.mean())

        lift = self.lift(beta_c, beta_t)

        prob = self.calc_prob(beta_c, beta_t)

        if verbose > 0:
            print(f"Экспериментальная группа имеет лифт в {lift*100:2.2f}% с вероятностью {prob*100:2.1f}%.")

        return prob, lift
