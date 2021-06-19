from scipy import stats as st
import statsmodels.api as sm
import matplotlib.pyplot as plt
import numpy as np
from operator import itemgetter
from numba import njit


def qq_plot(data, metric):
    sm.graphics.qqplot(data, st.norm, line='45', fit=True)
    plt.title(f"QQ plot for {metric} with normal distribution")
    plt.show()
    return


def normality_test(data, alpha=0.05, verbose=0):
    """
    Performs shapiro-wilk normality test https://en.wikipedia.org/wiki/Shapiro%E2%80%93Wilk_test

    :param data: (list) Sample of values
    :param alpha: (int. optional, default=0.05) confidence level
    :return: (bool) whether data is distributed normally or not with confidence of alpha
    """

    if verbose > 0:
        print('p-value of Hypothesis "Input data has normal distribution:"', st.shapiro(data)[1])

    if len(data) < 4000:
        if st.shapiro(data)[1] < alpha:
            if verbose > 0:
                print("Data is distributed normally")
            return True

        else:
            if verbose > 0:
                print("Data is not distributed normally")
            return False

    if st.normaltest(data)[1] < alpha:
        if verbose > 0:
            print("Data is not distributed normally")
        return False

    else:
        if verbose > 0:
            print("Data is distributed normally")
        return True

# @njit
def fisher_var(a, b, alpha=0.05):
    """
    Параметрический критерий Фишера для определения равенства дисперсий. В пределе сходится к распределению фишера.
    F(n-1, m-1), где m, n объемы выборок a, b соотв.
    Требует количественных данных и нормально распределенных выборок.

    :param a:
    :param b:
    :param alpha:
    :return:
    """

    "F = s1/s2, s1>=s2"
    # a = np.array(a)
    # b = np.array(b)
    n = len(a)
    m = len(b)

    s1 = 1/(n-1) * np.sum((a - np.mean(a))**2)
    s2 = 1/(m-1) * np.sum((a - np.mean(b))**2)

    if s1 >= s2:
        fisher_stat = s1/s2

    else:
        fisher_stat = s2/s1

    return fisher_stat


# @njit
def levene_var(*args, alpha=0.05, verbose=0):
    """
    Параметрический критерий Левене равенства двух дисперсий. В пределе сходится к распределению Фишера.
    F(1, m+n-2) где m, n объемы выборок a, b соотв.
    Требует количественных данных и нормально распределенных выборок.
    :param a: (list), First sample
    :param b: (list), Second sample
    :param alpha: (float, optional, default=0.05), alpha-level to reject null hypothesis
    :return:
    """
    stat_levene, p_levene = st.levene(*args)


    if verbose > 0:
        print('p-value of Hypothesis "Variances are equal:"', p_levene)

    if p_levene < alpha:
        if verbose > 0:
            print("Samples do not have same variance")
        return True

    else:
        if verbose > 0:
            print("Samples have same variance")
        return False


def mood_var(a, b, alpha=0.05, verbose=1):
    """
    Критерий равенства дисперсий Муда. Работает для выборок разных размеров, на количественных и порядковых данных.
    Не требует нормальности распределения выборок.


    :param a: (list), First sample
    :param b: (list), Second sample
    :param alpha: (float, optional, default=0.05), alpha-level to reject null hypothesis

    :return: Если n > 10 и m > 10, то возвращается сопряженная статистика муда,
    которая в пределе сходится к нормальному распределению. Иначе возвращается обычная статистика
    """

    mood_stat, p_value = st.mood(a, b)

    if verbose > 0:
        print('p-value of Hypothesis "Variances are equal:"', p_value)

    if p_value < alpha:
        if verbose > 0:
            print("Samples do not have same variance")
        return True

    else:
        if verbose > 0:
            print("Samples have same variance")
        return False


def independent_ttest(a, b, alpha=0.05, verbose=0, log=False, **kwargs):

    if log == False:
        stat, p_value = st.ttest_ind(a, b, **kwargs)

    else:
        stat, p_value = st.ttest_ind(np.log(a), np.log(b), **kwargs)

    if verbose > 0:
        print('p-value of null Hypothesis being wrong', p_value)

    if p_value < alpha:
        if verbose > 0:
            print("Reject null Hypothesis")

    else:
        if verbose > 0:
            print("Can not reject null Hypothesis")

    return p_value


def mann_whitneyu_test(a, b, alpha=0.05, verbose=0, **kwargs):

    stat, p_value = st.mannwhitneyu(a, b, **kwargs)

    if verbose > 0:
        print('p-value of null Hypothesis being wrong', p_value)

    if p_value < alpha:
        if verbose > 0:
            print("Reject null Hypothesis")

    else:
        if verbose > 0:
            print("Can not reject null Hypothesis")
    return p_value


# @njit
def lift(before, after):

    return (np.sum(after) - np.sum(before)) / np.sum(before)


# @njit
def bootstrap(data, sample_num, sample_size):
    res = np.array([])
    for i in range(sample_num):
        sample_mean = np.mean(np.random.choice(data, size=sample_size))
        res = np.append(res, sample_mean)

    return res


def kruskal_wallis(*args, nan_policy='propagate', alpha=0.05, verbose=0):

    stat, p_value = st.kruskal(*args, nan_policy=nan_policy)

    if verbose > 0:
        print('p-value of null Hypothesis being wrong', p_value)

    if p_value < alpha:
        if verbose > 0:
            print("Reject null Hypothesis")
        return True

    else:
        if verbose > 0:
            print("Can not reject null Hypothesis")
        return False


def z_test_ratio(successes1, successes2, trials1, trials2, alpha=0.05, verbose=0):
    """
    Z-test for binary variable. Null hypothesis H0: ratio in two groups is equal.

    :param successes1: (list), successes in first group
    :param successes2: (list), successes in second group
    :param trials1: (list), all trials in first group
    :param trials2: (list), all trials in first group
    :param alpha: (float, optional, default=0.05), alpha level to reject H0
    :param verbose: (int, optional, default=1), whether to print results
    :return: (bool), whether to reject H0
    """

    p1 = successes1 / trials1
    p2 = successes2 / trials2

    p_combined = (successes1 + successes2) / (trials1 + trials2)
    distr = st.norm(0, 1)
    z_value = (p1 - p2) / np.sqrt(p_combined * (1 - p_combined) * (1 / trials1 + 1 / trials2))

    p_value = (1 - distr.cdf(abs(z_value))) * 2

    if verbose > 0:
        print('p-value: ', p_value)

    if p_value < alpha:
        if verbose > 0:
            print("Reject null hypothesis, ratio in groups has statistically significant difference")
        return True
    else:
        if verbose > 0:
            print("Can not eject null hypothesis, ratio in groups has no statistically significant difference")
        return False
