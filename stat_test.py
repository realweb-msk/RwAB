from scipy import stats as st
import numpy as np
from operator import itemgetter
from numba import njit


def normality_test(data, alpha=0.05, verbose=1):
    """
    Performs shapiro-wilk normality test https://en.wikipedia.org/wiki/Shapiro%E2%80%93Wilk_test

    :param data: (list) Sample of values
    :param alpha: (int. optional, default=0.05) confidence level
    :return: (bool) whether data is distributed normally or not with confidence of alpha
    """

    if verbose > 0:
        print('p-value of Hypothesis "Input data has normal distribution:"', st.shapiro(data)[1])

    if st.shapiro(data)[1] < alpha:
        if verbose > 0:
            print("Data is distributed normally")
        return True

    else:
        if verbose > 0:
            print("Data is not distributed normally")
        return False

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
def levene_var(*args, alpha=0.05, verbose=1):
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
        print('p-value of Hypothesis "Distributions in input :"', p_levene)

    if p_levene < alpha:
        if verbose > 0:
            print("Samples do not have same variance")
        return True

    else:
        if verbose > 0:
            print("Samples have same variance")
        return False





# TODO: Fix this shit
def mood_var(a, b, alpha=0.05):
    """
    Критерий равенства дисперсий Муда. Работает для выборок разных размеров, на количественных и порядковых данных.
    Не требует нормальности распределения выборок.


    :param a: (list), First sample
    :param b: (list), Second sample
    :param alpha: (float, optional, default=0.05), alpha-level to reject null hypothesis

    :return: Если n > 10 и m > 10, то возвращается сопряженная статистика муда,
    которая в пределе сходится к нормальному распределению. Иначе возвращается обычная статистика
    """

    n = len(a)
    m = len(b)

    # Sort and rank
    a_dict = {'a'+str(i+1): v for i, v in zip(range(n), a)}
    b_dict = {'b'+str(i+1): v for i, v in zip(range(m), b)}
    a_dict.update(b_dict)
    a_dict = sorted(a_dict.items(), key=itemgetter(1))

    rank_dict = {k: r for k, r in a_dict}

    rank_list = []
    for k, v in rank_dict.items():
        if n <= m and 'a' in k:
            rank_list.append(v)

        elif n > m and 'b' in k:
            rank_list.append(v)

    mood_stat = np.sum((np.array(rank_list) - (m+n+1) / 2) ** 2)

    # Only when n > 10 and m > 10
    mood_stat_adjoint = (
            (mood_stat - m * (m+n+1) * (m+n-1) / 12 + 0.5) /
            (np.sqrt(m * n * (m+n+1) * (m+n+2) * (m+n-2)) / 180)
                         )

    if n > 10 and m > 10:
        return mood_stat_adjoint

    return mood_stat

