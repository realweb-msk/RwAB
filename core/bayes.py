import numpy as np
from math import lgamma
from scipy.stats import beta as beta_distr


class BayesTest:
    """
    Класс для Байессовского АБ теста. Применяется для оценки целевой метрики теста как бинарной величины.
    Основан на Бета-распределении и гамма функции. Подробнее: https://craftappmobile.com/bayesian-ab-testing-part-1/

    :param conversions_ctrl: Кол-во конверсий в контрольной группе
    :param conversions_test: Кол-во конверсий в экспериментальной группе
    :param impressions_ctrl: Кол-во показов в контрольной группе
    :param impressions_test:Кол-во показов в экспериментальной группе
    """

    def __init__(self, conversions_ctrl, conversions_test,  impressions_ctrl, impressions_test):
        self.impressions_ctrl = impressions_ctrl
        self.conversions_ctrl = conversions_ctrl
        self.impressions_test = impressions_test
        self.conversions_test = conversions_test

    def h(self, a, b, c, d):
        num = lgamma(a + c) + lgamma(b + d) + lgamma(a + b) + lgamma(c + d)
        den = (lgamma(a) + lgamma(b) + lgamma(c) + lgamma(d)
               + lgamma(a + b + c + d)
               )
        return np.exp(num - den)

    def g0(self, a, b, c):
        return np.exp(lgamma(a + b) + lgamma(a + c) -
                      (lgamma(a + b + c) + lgamma(a)))

    def calc(self, a, b, c, d):
        while d > 1:
            d -= 1
            yield self.h(a, b, c, d) / d

    def g(self, a, b, c, d):
        return self.g0(a, b, c) + sum(self.calc(a, b, c, d))

    def calc_prob(self, beta1, beta2):
        return self.g(beta1.args[0], beta1.args[1], beta2.args[0], beta2.args[1])

    @staticmethod
    def beta(impressions, conversions):
        a, b = conversions+1, impressions-conversions+1

        return beta_distr(a, b)

    @staticmethod
    def lift(before, after):
        return (after.mean() - before.mean()) / after.mean()

    def bayes_prob(self, verbose=0):
        beta_c = self.beta(self.impressions_ctrl, self.conversions_ctrl)
        beta_t = self.beta(self.impressions_test, self.conversions_test)

        if verbose > 0:
            print(beta_c.mean())
            print(beta_t.mean())

        lift = self.lift(beta_c, beta_t)
        prob = self.calc_prob(beta_c, beta_t)

        if prob < 0.3:
            prob = self.calc_prob(beta_t, beta_c)
            lift = self.lift(beta_t, beta_c)

        if verbose > 0:
            print(f"Экспериментальная группа имеет лифт в {lift*100:2.2f}% с вероятностью {prob*100:2.1f}%.")

        return prob, lift
