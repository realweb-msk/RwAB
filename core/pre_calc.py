# All credits to belong to Kasylin - https://github.com/Kasylin

"""
s_v = statsmodels.__version__.split('.')
if (float(s_v[1])==12 and float(s_v[2])<1) or (float(s_v[1])<12):
    get_ipython().system('pip install statsmodels --upgrade')
    s_v = statsmodels.__version__.split('.')
    if (float(s_v[1])==12 and float(s_v[2])<1) or (float(s_v[1])<12):
        print('statsmodels needs to be at least v0.12.1')
"""
import numpy as np
import pandas as pd
import statsmodels.stats.proportion as stp
import statsmodels.stats.power as power
from statsmodels.stats.weightstats import ttest_ind
from sklearn.linear_model import LinearRegression
from core import stat_test
from plotly import graph_objects as go
from plotly.subplots import make_subplots
import plotly

cols = plotly.colors.DEFAULT_PLOTLY_COLORS

"""import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir) """


def monte_carlo_power_2(baseline_data, weekly_sessions, effect_size, N,
                        alpha=0.05, power=0.8,
                        num_simulations=1000,
                        method='CR z-test',
                        alternative='two-sided', verbose=0):
    # 2: make variant data based on sample_data and effect_size
    # effect_size = (mean1 - mean2)/std
    # lift = mean1-mean2 = effect_size*std
    # lifted_data = control_data - lift
    lifted_data = baseline_data - effect_size * baseline_data.std()

    significance_results = []
    for j in range(0, num_simulations):
        # 1: make a sample of needed size from the sample_data
        control_data = np.random.choice(baseline_data, size=N)

        # 3: make a sample of needed size from the lifted data,
        # which will be the variant data
        variant_data = np.random.choice(lifted_data, size=N)

        if method == 't-test':
            # Use Welch's t-test, make no assumptions on tests for equal variances
            test_result = ttest_ind(control_data, variant_data,
                                    alternative=alternative, usevar='unequal')
            # Test for significance
            significance_results.append(test_result[1] <= alpha)
        elif method == 'Kruskal-Wallis':
            test_result = stat_test.kruskal_wallis(control_data, variant_data, alpha=alpha,
                                                   verbose=0)
            # Test for significance
            significance_results.append(test_result)
        elif method == 'Mann-Whitney':
            test_result = stat_test.mann_whitneyu_test(control_data, variant_data, alpha=alpha,
                                                       verbose=0)
            # Test for significance
            significance_results.append(test_result)
        """elif method=='Mood':
          test_result = mood_var(control_data, variant_data, alpha=alpha,
                                       verbose=0)
          # Test for significance
          significance_results.append(test_result)
        elif method=='Levene':
          test_result = levene_var(control_data, variant_data, alpha=alpha,
                                       verbose=0)
          # Test for significance
          significance_results.append(test_result)"""
    # The power is the number of times we have a significant result
    # as we are assuming the alternative hypothesis is true
    return np.mean(significance_results)


def monte_carlo_sample_size(baseline_data, weekly_sessions, effect_size,
                            method='CR z-test',
                            min_weeks=0.5, max_weeks=4,
                            alpha=0.05, power=0.8,
                            num_simulations=1000,
                            alternative='two-sided', verbose=0):
    """
    Monte Carlo power algorithm:
      Input: baseline data, some lift value
      Output: sample size / power

    Input: baseline data, method
    Output: lifts / weeks graph

    Algorithm:
    1. Set range of lifts
    2. Simulate power calculations for all lifts
    3. See at what sample size each lift value has power=80%
    """
    # for now effect_size is normalised like for the stats.power function

    sample_sizes = np.arange(0.5, max_weeks + 0.5, 0.5) * weekly_sessions  # Sample sizes we will test over

    for i in range(0, len(sample_sizes)):
        N = int(sample_sizes[i])
        # get power for a given sample size
        b = monte_carlo_power_2(baseline_data, weekly_sessions, effect_size, N=N,
                                alpha=alpha, power=power,
                                num_simulations=num_simulations,
                                method=method,
                                alternative=alternative, verbose=verbose)
        min_sample_size = N
        if b >= power:

            break
        else:
            continue

    return min_sample_size


def lifts_n_regression(lifts, sample_sizes, baseline_sessions, max_weeks=4):
    """
    Calculates optimal minimal sample size for experiment given two arrays:
    lifts and minimal sample sizes (like on the graph)

    Algorithm:
    1) regress function (linearized y=a*x^b is ln(y)=ln(a)+b*ln(x),
                          where y=exp(ln(y)), a=exp(ln(a)))
    2) calculate tangent slopes at different points:
       tg(alpha) = a*b*x^(b-1)
    3) determine optimal sample_size the following way:
    """
    reg = LinearRegression().fit(X=np.log(np.asarray(sample_sizes).reshape(-1, 1)), y=np.log(lifts))
    print('score: ', reg.score(X=np.log(np.asarray(sample_sizes).reshape(-1, 1)), y=np.log(lifts)))
    a = np.exp(reg.intercept_)
    b = reg.coef_[0]
    # print(a,reg.coef_)

    # frequency - как часто проверять угол наклона касательной
    # пока условно считаем, что каждый день
    freq = max_weeks * 7

    # sample sizes at which we'll calculate tangent slope
    samples = np.linspace(min(sample_sizes) - baseline_sessions / freq * (freq - 1), baseline_sessions * max_weeks,
                          freq)

    # tangent_slope = (a*b)*(samples ** (b-1))

    y = a * np.power(samples, b)

    return samples, y


def lifts_weeks_graph(baseline_sessions, cr_name='CR',
                      baseline_conversions=None, baseline_cr=None,
                      # baseline_mean=None, baseline_std=None,
                      baseline_data=None,
                      method='CR', a=0.05, b=0.8, ratio=1,
                      num_simulations=2000,
                      max_lift=None, max_weeks=4,
                      plot_title='Desktop', plot_width=800,
                      plotly_template='none',
                      interpolate=True):
    """
    Creates a plotly.express graph that has number of weeks on the X-axis and
    effect size on the Y-axis

    Parameters:
    - a - alpha - significance level
    - b - beta -
    - max_lift - maximum lift that will be showed on the graph
    - max_weeks - maximum number of weeks - max value for X-axis
    - plot_title - plot title
    - plot_width - plot width
    - plotly_template - https://plotly.com/python/templates/
    - method - method. There are 3 options:
      - 'CR' - for binary data. Performs a z-test (default - one-sided)
      - 'Students t-test formula'
      - 'Monte-Carlo t-test'
      - 'Monte-Carlo Kruskal-Wallis'
      - 'Monte-Carlo Mann-Whitney'
      - 'Monte-Carlo Levene'

    Parameters for the 'CR' method:
    - baseline_sessions - number of sessions in a week for the first variant (control group)
    - cr_name - name that is displayed on the graph
    - baseline_conversions - number of conversions in a week for the first variant
    - baseline_cr - baseline conversion rate
    - ratio - ratio of variants (variant 2 / variant 1)

    Parameters for the 'Students t-test formula' and 'Monte-Carlo...' methods:
    - baseline_data - baseline data

    Parameters for the 'Monte-Carlo...' methods:
    - num_simulations - number of simulations
    - interpolate - if the results of simulation have to be interpolated
    """

    # If conversions are a binary variable:
    if method == 'CR':
        # if min(baseline_sessions) <=

        # check if baseline_cr or baseline_conversions was defined
        if baseline_cr == None:
            # TO DO: account for the case when baseline_sessions and
            # baseline_converions aren't the same length
            try:
                if type(baseline_conversions) is float:
                    baseline_cr = [baseline_conversions / baseline_sessions]
                elif type(baseline_conversions) is list:
                    baseline_cr = baseline_conversions / baseline_sessions
            except NameError:
                print('Error: parameter baseline_conversions is undefined')

        if type(baseline_cr) is float:
            baseline_cr = [baseline_cr]
        if type(cr_name) is str:
            cr_name = [cr_name]

        if (type(baseline_sessions) is float) | (
                type(baseline_sessions) is int):
            if max_lift == None:
                if float(baseline_sessions) >= 10000:
                    max_lift = 0.02
                elif float(baseline_sessions) >= 5000:
                    max_lift = 0.03
                elif float(baseline_sessions) >= 1000:
                    max_lift = 0.05
                else:
                    max_lift = 0.1
            lifts = np.arange(0.0005, max_lift + 0.0005, 0.0005).tolist()

        elif (type(baseline_sessions) is list) & (
                len(baseline_sessions) == len(baseline_cr)):
            if max_lift == None:
                if min(baseline_sessions) >= 10000:
                    max_lift = 0.02
                elif min(baseline_sessions) >= 5000:
                    max_lift = 0.03
                elif min(baseline_sessions) >= 1000:
                    max_lift = 0.05
                else:
                    max_lift = 0.1

            lifts = np.arange(0.0005, max_lift + 0.0005, 0.0005).tolist()

        for j in range(len(baseline_cr)):
            locals()['p%s' % j] = baseline_cr[j]
            globals()['n%s' % j] = []
            # locals()['n_diff_%s' % j] = []
            for i in range(len(lifts)):
                eval('n%s' % j).append(power.NormalIndPower().solve_power(
                    effect_size=stp.proportion_effectsize(eval('p%s' % j) + lifts[i],
                                                          eval('p%s' % j)),
                    nobs1=None, alpha=a, power=b, ratio=ratio,
                    alternative='larger'))

            """for i in range(1,len(lifts)):
              eval('n_diff_%s' % j).append(
                  (eval('n%s' % j)[-(i+1)]-eval('n%s' % j)[-i])/eval('n%s' % j)[-i])
          
            print(eval('n%s' % j))
            print(eval('n_diff_%s' % j))"""

        # plot graph
        fig = make_subplots(rows=1, cols=1, subplot_titles=(plot_title, 'lfllf'))

        # if there's one baseline weekly number of sessions:
        if (type(baseline_sessions) is float) | (
                type(baseline_sessions) is int):
            for i in range(len(baseline_cr)):
                fig.add_trace(go.Scatter(
                    x=[(x / baseline_sessions) for x in eval('n' + str(i))],
                    y=[x * 100 for x in lifts],
                    mode='lines',
                    name=cr_name[i] + '=' + str(round(baseline_cr[i] * 100, 2)) + '%',
                    legendgroup='group1', showlegend=True), row=1, col=1)

            if baseline_sessions >= 1000:
                fig.update_layout(height=500, width=800,
                                  title_text="Лифт при power=" + str(b * 100) +
                                             "%, \u03B1=" +
                                             str(a * 100) + "%, weekly sessions=" +
                                             str(baseline_sessions / 1000) + 'k',
                                  template=plotly_template)
            else:
                fig.update_layout(height=500, width=800,
                                  title_text="Лифт при power=" + str(b * 100) +
                                             "%, \u03B1=" +
                                             str(a * 100) + "%, weekly sessions=" + str(baseline_sessions),
                                  template=plotly_template)

        elif (type(baseline_sessions) is list) & (
                len(baseline_sessions) == len(baseline_cr)):
            for i in range(len(baseline_cr)):
                if baseline_sessions[i] >= 1000:
                    fig.add_trace(go.Scatter(
                        x=[(x / baseline_sessions[i]) for x in eval('n' + str(i))],
                        y=[x * 100 for x in lifts],
                        mode='lines',
                        name=cr_name[i] + '=' + str(round(baseline_cr[i] * 100, 2)) +
                             '%, weekly sessions=' +
                             str(baseline_sessions[i] / 1000) + 'k',
                        legendgroup='group1', showlegend=True), row=1, col=1)
                else:
                    fig.add_trace(go.Scatter(
                        x=[(x / baseline_sessions[i]) for x in eval('n' + str(i))],
                        y=[x * 100 for x in lifts],
                        mode='lines',
                        name=cr_name[i] + '=' + str(round(baseline_cr[i] * 100, 2)) +
                             '%, weekly sessions=' + str(baseline_sessions[i]),
                        legendgroup='group1', showlegend=True), row=1, col=1)
            fig.update_layout(height=500, width=plot_width,
                              title_text="Лифт при power=" + str(b * 100) +
                                         "%, \u03B1=" +
                                         str(a * 100) + "%", template=plotly_template)

        fig.update_xaxes(title_text="Недели~", row=1, col=1, range=[0, max_weeks])
        fig.update_yaxes(title_text="Лифт в абс. значениях (%)", row=1, col=1)
        fig.show()



    #############################################################################
    # If Student's t-test is required:
    elif method == 'Students t-test formula':
        baseline_mean = [baseline_data.mean()]

        # TO DO: include max_lift
        lifts = np.arange(0.02, 0.5 + 0.005, 0.005).tolist()

        n = []
        for i in range(len(lifts)):
            n.append(power.tt_ind_solve_power(
                effect_size=lifts[i],
                nobs1=None, ratio=1, alpha=0.05, power=0.8))

        # plot graph
        fig = make_subplots(rows=1, cols=1, subplot_titles=(plot_title, 'lfllf'))

        for i in range(len(baseline_mean)):
            fig.add_trace(go.Scatter(
                x=[(x / baseline_sessions) for x in n],
                y=lifts,
                mode='lines',
                name='mean=' + str(round(baseline_mean[i], 2)),
                legendgroup='group1', showlegend=True), row=1, col=1)

            if baseline_sessions >= 1000:
                fig.update_layout(height=500, width=800,
                                  title_text="Лифт при power=" + str(b * 100) +
                                             "%, \u03B1=" +
                                             str(a * 100) + "%, weekly sessions=" +
                                             str(baseline_sessions / 1000) + 'k',
                                  template=plotly_template)
            else:
                fig.update_layout(height=500, width=800,
                                  title_text="Лифт при power=" + str(b * 100) +
                                             "%, \u03B1=" +
                                             str(a * 100) + "%, weekly sessions=" + str(baseline_sessions),
                                  template=plotly_template)
        fig.update_xaxes(title_text="Недели~", row=1, col=1, range=[0, max_weeks])
        fig.update_yaxes(title_text="(A-B)/std", row=1, col=1)
        fig.show()

    ##############################################################################
    elif method[0:11] == 'Monte-Carlo':
        baseline_mean = baseline_data.mean()

        # TO DO: include max_lift
        lifts = np.arange(0.02, 0.5 + 0.005, 0.005).tolist()

        n = []
        for i in range(len(lifts)):
            n.append(monte_carlo_sample_size(
                baseline_data, baseline_sessions, lifts[i],
                num_simulations=num_simulations,
                method=method[12:]))

        if interpolate:
            lifts_n = pd.DataFrame(lifts, columns=['lifts'])
            lifts_n['n'] = n
            lifts_n = lifts_n.groupby('n').min().reset_index()
            # n, lifts = lifts_n['n'], lifts_n['lifts']
            n, lifts = lifts_n_regression(lifts_n['lifts'], lifts_n['n'],
                                          baseline_sessions=baseline_sessions,
                                          max_weeks=max_weeks)

        # plot graph
        fig = make_subplots(rows=1, cols=1, subplot_titles=(plot_title, 'lfllf'))

        fig.add_trace(go.Scatter(
            x=[(x / baseline_sessions) for x in n],
            y=lifts,
            mode='lines',
            name='mean=' + str(round(baseline_mean, 2)),
            legendgroup='group1', showlegend=True, line_shape='spline'
        ), row=1, col=1)

        if baseline_sessions >= 1000:
            fig.update_layout(height=500, width=800,
                              title_text="Лифт при power=" + str(b * 100) +
                                         "%, \u03B1=" +
                                         str(a * 100) + "%, weekly sessions=" +
                                         str(baseline_sessions / 1000) + 'k',
                              template=plotly_template)
        else:
            fig.update_layout(height=500, width=800,
                              title_text="Лифт при power=" + str(b * 100) +
                                         "%, \u03B1=" +
                                         str(a * 100) + "%, weekly sessions=" + str(baseline_sessions),
                              template=plotly_template)
        fig.update_xaxes(title_text="Недели~", row=1, col=1, range=[0, max_weeks])
        fig.update_yaxes(title_text="(A-B)/std", row=1, col=1, range=[0, max(lifts)])
        fig.show()


def precalc(baseline_sessions,
            baseline_conversions=None, baseline_cr=None,
            baseline_mean=None, baseline_std=None,
            baseline_data=None,
            method='CR', a=0.05, b=0.8, ratio=1,
            num_simulations=2000,
            max_lift=None, max_weeks=4,
            plot_title='Desktop',
            plotly_template='none',
            interpolate=False):
    # If conversions are a binary variable:
    if method == 'CR':
        # if min(baseline_sessions) <=

        # check if baseline_cr or baseline_conversions was defined

        if baseline_cr == None:
            # TO DO: account for the case when baseline_sessions and
            # baseline_converions aren't the same length
            try:
                if type(baseline_conversions) is float:
                    baseline_cr = [baseline_conversions / baseline_sessions]
                elif type(baseline_conversions) is list:
                    baseline_cr = baseline_conversions / baseline_sessions
            except NameError:
                print('Error: parameter baseline_conversions is undefined')

        if type(baseline_cr) is float:
            baseline_cr = [baseline_cr]

        if (type(baseline_sessions) is float) | (
                type(baseline_sessions) is int):
            if max_lift == None:
                if float(baseline_sessions) >= 10000:
                    max_lift = 0.02
                elif float(baseline_sessions) >= 5000:
                    max_lift = 0.03
                elif float(baseline_sessions) >= 1000:
                    max_lift = 0.05
                else:
                    max_lift = 0.1
            lifts = np.arange(0.0005, max_lift + 0.0005, 0.0005).tolist()

        elif (type(baseline_sessions) is list) & (
                len(baseline_sessions) == len(baseline_cr)):
            if max_lift == None:
                if min(baseline_sessions) >= 10000:
                    max_lift = 0.02
                elif min(baseline_sessions) >= 5000:
                    max_lift = 0.03
                elif min(baseline_sessions) >= 1000:
                    max_lift = 0.05
                else:
                    max_lift = 0.1

            lifts = np.arange(0.0005, max_lift + 0.0005, 0.0005).tolist()

        for j in range(len(baseline_cr)):
            locals()['p%s' % j] = baseline_cr[j]
            globals()['n%s' % j] = []
            # locals()['n_diff_%s' % j] = []
            for i in range(len(lifts)):
                eval('n%s' % j).append(power.NormalIndPower().solve_power(
                    effect_size=stp.proportion_effectsize(eval('p%s' % j) + lifts[i],
                                                          eval('p%s' % j)),
                    nobs1=None, alpha=a, power=b, ratio=ratio,
                    alternative='larger'))

        results = pd.concat([pd.Series(lifts)] + [pd.Series(eval('n' + str(i))) for i in range(len(baseline_cr))],
                            axis='columns')
        results.columns = ['lifts'] + ['n' + str(i) for i in range(len(baseline_cr))]
        return results

    #############################################################################
    # If Student's t-test is required:
    elif method == 'Students t-test formula':
        baseline_mean = [baseline_data.mean()]

        # TO DO: include max_lift and several vars (baseline means)
        lifts = np.arange(0.02, 0.5 + 0.005, 0.005).tolist()

        n = []
        for i in range(len(lifts)):
            n.append(power.tt_ind_solve_power(
                effect_size=lifts[i],
                nobs1=None, ratio=1, alpha=0.05, power=0.8))

        results = pd.DataFrame([lifts, n],
                               columns=['lifts', 'n0'])

        return results

    ##############################################################################
    elif method[0:11] == 'Monte-Carlo':
        baseline_mean = baseline_data.mean()

        # TO DO: include max_lift
        lifts = np.arange(0.02, 0.5 + 0.005, 0.005).tolist()

        n = []
        for i in range(len(lifts)):
            n.append(monte_carlo_sample_size(
                baseline_data, baseline_sessions, lifts[i],
                num_simulations=num_simulations,
                method=method[12:]))

        if interpolate:
            lifts_n = pd.DataFrame(lifts, columns=['lifts'])
            lifts_n['n'] = n
            lifts_n = lifts_n.groupby('n').min().reset_index()
            # n, lifts = lifts_n['n'], lifts_n['lifts']
            n, lifts = lifts_n_regression(lifts_n['lifts'], lifts_n['n'],
                                          baseline_sessions=baseline_sessions,
                                          max_weeks=max_weeks)

        results = pd.DataFrame([lifts, n],
                               columns=['lifts', 'n0'])

        return results


def plot_precalc(baseline_sessions, cr_name='CR',
                 baseline_conversions=None, baseline_cr=None,
                 baseline_mean=None, baseline_std=None,
                 baseline_data=None,
                 method='CR', a=0.05, b=0.8, ratio=1,
                 num_simulations=2000,
                 max_lift=None, max_weeks=4,
                 plot_title='Desktop', plot_width=800,
                 plotly_template='none',
                 interpolate=True,
                 return_type='graph'):
    # get precalc results:
    results = precalc(baseline_sessions=baseline_sessions,
                      baseline_conversions=baseline_conversions, baseline_cr=baseline_cr,
                      baseline_mean=baseline_mean, baseline_std=baseline_std,
                      baseline_data=baseline_data,
                      method=method, a=a, b=b, ratio=ratio,
                      num_simulations=num_simulations,
                      max_lift=max_lift, max_weeks=max_weeks,
                      interpolate=interpolate)

    if return_type == 'table':
        return results
    elif return_type == 'graph':
        lifts = results['lifts']
        for i in range(len(results.columns) - 1):
            globals()['n%s' % i] = results['n' + str(i)]

        # If conversions are a binary variable:
        if method == 'CR':
            if baseline_cr == None:
                # TO DO: account for the case when baseline_sessions and
                # baseline_converions aren't the same length
                try:
                    if type(baseline_conversions) is float:
                        baseline_cr = [baseline_conversions / baseline_sessions]
                    elif type(baseline_conversions) is list:
                        baseline_cr = baseline_conversions / baseline_sessions
                except NameError:
                    print('Error: parameter baseline_conversions is undefined')

            if type(baseline_cr) is float:
                baseline_cr = [baseline_cr]
            # plot graph
            fig = make_subplots(rows=1, cols=1, subplot_titles=(plot_title, 'lfllf'))

            # if there's one baseline weekly number of sessions:
            if (type(baseline_sessions) is float) | (
                    type(baseline_sessions) is int):
                for i in range(len(baseline_cr)):
                    fig.add_trace(go.Scatter(
                        x=[(x / baseline_sessions) for x in eval('n' + str(i))],
                        y=[x * 100 for x in lifts],
                        mode='lines',
                        name='CR=' + str(round(baseline_cr[i] * 100, 2)) + '%',
                        legendgroup='group1', showlegend=True), row=1, col=1)

                if baseline_sessions >= 1000:
                    fig.update_layout(height=500, width=800,
                                      title_text="Лифт при power=" + str(b * 100) +
                                                 "%, \u03B1=" +
                                                 str(a * 100) + "%, weekly sessions=" +
                                                 str(baseline_sessions / 1000) + 'k',
                                      template=plotly_template)
                else:
                    fig.update_layout(height=500, width=800,
                                      title_text="Лифт при power=" + str(b * 100) +
                                                 "%, \u03B1=" +
                                                 str(a * 100) + "%, weekly sessions=" + str(baseline_sessions),
                                      template=plotly_template)

            elif (type(baseline_sessions) is list) & (
                    len(baseline_sessions) == len(baseline_cr)):
                for i in range(len(baseline_cr)):
                    if baseline_sessions[i] >= 1000:
                        fig.add_trace(go.Scatter(
                            x=[(x / baseline_sessions[i]) for x in eval('n' + str(i))],
                            y=[x * 100 for x in lifts],
                            mode='lines',
                            name='CR=' + str(round(baseline_cr[i] * 100, 2)) +
                                 '%, weekly sessions=' +
                                 str(baseline_sessions[i] / 1000) + 'k',
                            legendgroup='group1', showlegend=True), row=1, col=1)
                    else:
                        fig.add_trace(go.Scatter(
                            x=[(x / baseline_sessions[i]) for x in eval('n' + str(i))],
                            y=[x * 100 for x in lifts],
                            mode='lines',
                            name='CR=' + str(round(baseline_cr[i] * 100, 2)) +
                                 '%, weekly sessions=' + str(baseline_sessions[i]),
                            legendgroup='group1', showlegend=True), row=1, col=1)
                fig.update_layout(height=500, width=800,
                                  title_text="Лифт при power=" + str(b * 100) +
                                             "%, \u03B1=" +
                                             str(a * 100) + "%", template=plotly_template)

            fig.update_xaxes(title_text="Недели~", row=1, col=1, range=[0, max_weeks])
            fig.update_yaxes(title_text="Лифт в абс. значениях (%)", row=1, col=1)
            fig.show()


        #############################################################################
        # If Student's t-test is required:
        elif method == 'Students t-test formula':

            # plot graph
            fig = make_subplots(rows=1, cols=1, subplot_titles=(plot_title, 'lfllf'))

            for i in range(len(baseline_mean)):
                fig.add_trace(go.Scatter(
                    x=[(x / baseline_sessions) for x in eval('n' + str(i))],
                    y=lifts,
                    mode='lines',
                    name='mean=' + str(round(baseline_mean[i], 2)),
                    legendgroup='group1', showlegend=True), row=1, col=1)

                if baseline_sessions >= 1000:
                    fig.update_layout(height=500, width=800,
                                      title_text="Лифт при power=" + str(b * 100) +
                                                 "%, \u03B1=" +
                                                 str(a * 100) + "%, weekly sessions=" +
                                                 str(baseline_sessions / 1000) + 'k',
                                      template=plotly_template)
                else:
                    fig.update_layout(height=500, width=800,
                                      title_text="Лифт при power=" + str(b * 100) +
                                                 "%, \u03B1=" +
                                                 str(a * 100) + "%, weekly sessions=" + str(baseline_sessions),
                                      template=plotly_template)
            fig.update_xaxes(title_text="Недели~", row=1, col=1, range=[0, max_weeks])
            fig.update_yaxes(title_text="(A-B)/std", row=1, col=1)
            fig.show()

        ##############################################################################
        elif method[0:11] == 'Monte-Carlo':

            # plot graph
            fig = make_subplots(rows=1, cols=1, subplot_titles=(plot_title, 'lfllf'))

            fig.add_trace(go.Scatter(
                x=[(x / baseline_sessions) for x in eval('n' + str(i))],
                y=lifts,
                mode='lines',
                name='mean=' + str(round(baseline_mean, 2)),
                legendgroup='group1', showlegend=True, line_shape='spline'
            ), row=1, col=1)

            if baseline_sessions >= 1000:
                fig.update_layout(height=500, width=800,
                                  title_text="Лифт при power=" + str(b * 100) +
                                             "%, \u03B1=" +
                                             str(a * 100) + "%, weekly sessions=" +
                                             str(baseline_sessions / 1000) + 'k',
                                  template=plotly_template)
            else:
                fig.update_layout(height=500, width=800,
                                  title_text="Лифт при power=" + str(b * 100) +
                                             "%, \u03B1=" +
                                             str(a * 100) + "%, weekly sessions=" + str(baseline_sessions),
                                  template=plotly_template)
            fig.update_xaxes(title_text="Недели~", row=1, col=1, range=[0, max_weeks])
            fig.update_yaxes(title_text="(A-B)/std", row=1, col=1, range=[0, max(lifts)])
            fig.show()


# TODO: Finish compare_results
def compare_results():
    """
    Compares results of plot_precalc(..., return_type='table')

    """


# test for binary variables
"""
lifts_weeks_graph(baseline_cr=0.07, baseline_sessions=50000, 
                  ratio=1, plot_title='omg its working', method='CR', 
                  max_lift=0.05, max_weeks=4)

plot_precalc(baseline_cr=0.07, baseline_sessions=50000, 
                  ratio=1, plot_title='plot_precalc', method='CR', 
                  max_lift=0.05, max_weeks=4, return_type='graph')
"""

# test for continuous variables
# t-test standart formula

# t-test simulation
