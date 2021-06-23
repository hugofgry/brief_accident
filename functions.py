import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


#classification metrics
from sklearn.metrics import f1_score, jaccard_score, confusion_matrix, log_loss, recall_score, precision_score,\
precision_recall_curve, plot_confusion_matrix, plot_precision_recall_curve, roc_curve, roc_auc_score, \
plot_roc_curve

#preprocessing and model selection
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, RepeatedKFold, train_test_split, cross_val_score

#classification models 
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

#regression models
from sklearn.svm import LinearSVR

def df_info(df):
    import pandas as pd
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    print(f"""
    Number of lines : {df.shape[0]}
    Number of columns: {df.shape[1]}
    """)

    numericals = [col for col in df.columns if df[col].dtype != 'object']
    categoricals = [col for col in df.columns if df[col].dtype == 'object']
    
    info = pd.DataFrame(columns = ['name', 'type', 'n_unique', 'unique', 'null%'])
    
    for col in df.columns:
        col_name = col
        col_type = df[col].dtypes
        num = len(df[col].unique())
        uni = []
        #unique for objects
        #range for numerics
        if col in categoricals:
            uni = list(df[col].unique())
        if col in numericals:
            uni = ["{:.2e}".format(df.describe().loc[minmax,col]) for minmax in ['min', 'mean','max']]
        nulls = round(df[col].isna().sum()/len(df)*100.,2)
    
        new_row = pd.Series(data={'name': col_name, 'type': col_type, 'n_unique': num, 'unique': uni, 'null%': nulls} , name='x')
        #append row to the dataframe
        info = info.append(new_row, ignore_index=True)
 
               
    display(info.sort_values(by='null%', ascending= False))


def plot_categorical_features(df, target, max_num_cat=25):
    """Parameters:
    df - data frame with different types of features with target,
    target - name of the column of dependent feature
    Output: display plots"""

    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np
    import matplotlib.gridspec as gridspec

    sns.color_palette("Paired")
    pal = sns.color_palette("Paired").as_hex()

    # if target is categorical

    #create df of categorical features
    ## Attention on condition
    case = 0  # target is numerical by default
    cat_features = [
        column for column in df.columns if df[column].dtypes == "object"
    ] + [
        column for column in df.columns
        if df[column].dtypes == "int" and len(df[column].unique()) < 20
    ]

    if target in cat_features:
        case = 1
        cat_features.remove(target)

    df_cat = df[cat_features]

    if len(cat_features) > 0:

        # gridspec inside gridspec
        num_plots = len(cat_features)
        #you can change number of columns
        cols = 3
        rows = num_plots // cols if num_plots % cols == 0 else num_plots // cols + 1

        fig = plt.figure(figsize=(cols * 5, rows * 3))
        gs0 = gridspec.GridSpec(rows, cols, figure=fig)
        for i, feature in enumerate(cat_features):
            gs00 = gridspec.GridSpecFromSubplotSpec(5, 1, subplot_spec=gs0[i])

            ax1 = fig.add_subplot(gs00[:-1, :])

            if case == 1:
                data_dict = {}
                for target_class in list(df[target].unique()):
                    data_dict[target_class] = []
                    df_target = df[df[target] == target_class]
                    for feature_category in list(df_cat[feature].unique()):
                        df_temp = df_target[df_target[feature] ==
                                            feature_category]
                        data_dict[target_class].append(len(df_temp))
                plotdata = pd.DataFrame(data_dict,
                                        index=list(df_cat[feature].unique()))
                plotdata.plot(kind="bar", stacked=True, color=pal, ax=ax1)
                ax1.set_xlabel(f"Classes in {feature}")
                ax1.set_ylabel("frequency")
                for tick in ax1.get_xticklabels():
                    tick.set_rotation(0)

            else:
                sns.boxplot(data=df, x=feature, y=target, ax=ax1)
                sns.stripplot(data=df,
                              x=feature,
                              y=target,
                              ax=ax1,
                              color=".25",
                              size=2)  #swarmplots
                ax1.set_title(f"{target} by {feature}")

    else:
        print('There are no categorical features in your dataset')    
        
        
def plot_numeric_features(df, max_num_cat = 25):
    
    """Parameters:
    plot_numerical_features(df, max_num_cat = 20)
    df     - data frame with different types of features with target,
    max_num_cat - number of classes in columns where the type is int by default = 25
    
    Output: display plots"""
    
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    import seaborn as sns
    sns.color_palette("Paired")
    #condition for categorical features is the same as in plot_categoric_features()
    cat_features = [column for column in df.columns if df[column].dtypes == "object" ] + \
    [column for column in df.columns if df[column].dtypes == "int" and len(df[column].unique())< max_num_cat]
    num_features = [col for col in df.columns if col not in cat_features]
    
    if len(num_features)>0:

        # gridspec inside gridspec
        num_plots = len(num_features)
        #you can change number of columns
        cols = 3 
        rows = num_plots//cols if num_plots%cols == 0 else num_plots//cols +1

        fig = plt.figure(figsize = (cols*5,rows*3))
        gs0 = gridspec.GridSpec(rows, cols, figure=fig)
        for i,feature in enumerate(num_features):
            gs00 = gridspec.GridSpecFromSubplotSpec(5, 1, subplot_spec=gs0[i])

            ax1 = fig.add_subplot(gs00[0, :])
            ax2 = fig.add_subplot(gs00[1:-1, :])

            sns.boxplot(data=df, x=feature, ax=ax1)
            sns.histplot(data=df, x=feature, kde=True, ax=ax2)

            #plt.subplots_adjust(wspace=0, hspace=0.1)
            ax1.set(xlabel='')
            ax1.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
            ax1.axis('off')
            
    else:
        print('There are no numerical features in your dataset')
        
def plot_res_corr(df, target):
    
    """Parameters:
    plot_numerical_features(df, max_num_cat = 20)
    df     - data frame with different types of features with target,
    max_num_cat - number of classes in columns where the type is int by default = 25
    
    Output: display plots"""
    
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    import seaborn as sns
    sns.color_palette("Paired")
    pal = sns.color_palette("Paired").as_hex()

    cat_features = [column for column in df.columns if df[column].dtypes == "object" ] 
    num_features = [col for col in df.columns if col not in cat_features]
    num_plots = len(num_features)-1
    
    
    if len(num_features)>0:

        # gridspec inside gridspec
        num_plots = len(num_features)
        #you can change number of columns
        cols = 3 
        rows = num_plots//cols if num_plots%cols == 0 else num_plots//cols +1

        fig = plt.figure(figsize = (cols*5,rows*6))
        gs0 = gridspec.GridSpec(rows, cols, figure=fig)
        i = 0
        for feature in num_features:
            if feature  != target:
                gs00 = gridspec.GridSpecFromSubplotSpec(7, 1, subplot_spec=gs0[i])
                ax1 = fig.add_subplot(gs00[0:-4, :])
                ax2 = fig.add_subplot(gs00[-4:-1, :])
                
                sns.regplot(data=df, x=feature, y=target, color = pal[3], ax=ax1)
                sns.residplot(data=df, x=feature, y=target, color = pal[4], ax = ax2)   
                i+=1
            
    else:
        print('There are no numerical features in your dataset')
 
        

def show_regression_model_metrics(model, X_test, y_test):

    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy  as np
    from sklearn.metrics import mean_squared_error, r2_score

    """Parameters:
    show_regression_model_metrics(model, X_test, y_test)
    model - regression model,
    X_test - test data set
    y_test
    Output: display plots"""
    # Get predictions
    predictions = model.predict(X_test)

    # Show metrics
    print('Metrics of y_test prediction:')
    mse = mean_squared_error(y_test, predictions)
    print("MSE:", mse)
    rmse = np.sqrt(mse)
    print("RMSE:", rmse)
    r2 = r2_score(y_test, predictions)
    print("R2:", r2)
    # Plot predicted vs actual

    fig = plt.figure(figsize = (14,9))
    plt.scatter(y_test, predictions)
    plt.xlabel(f'Actual')
    plt.ylabel(f'Predicted')
    plt.title('Predictions vs Actuals')
    z = np.polyfit(y_test, predictions, 1)
    p = np.poly1d(z)
    plt.plot(y_test, p(y_test), color='magenta')
    plt.show()
    
    
    
def show_classification_model_metrics(model, x_test, y_test):
    """Parameters:
    model: already fit classification model
    x_test: sample
    y_test: known values of the target of the sample
    Output:
    All metrics of the classification model, and 3 plots - confusion matrics,
    Precision-Recall Curve and ROC Curve"""

    from sklearn.metrics import jaccard_score, recall_score, precision_score, f1_score, log_loss,  \
    confusion_matrix, plot_confusion_matrix, precision_recall_curve, plot_precision_recall_curve, \
    roc_curve, roc_auc_score, plot_roc_curve, classification_report

    y_hat = model.predict(x_test)
    #dist_desision = model.decision_function(X_test)
    r = recall_score(y_test, y_hat)
    p = precision_score(y_test, y_hat)
    probs = model.predict_proba(x_test)
    prob_trans = probs.transpose()[0]
    auc = roc_auc_score(y_test, probs.transpose()[1])

    #     confusion_matrix(y_test, y_hat)
    #     precision_recall_curve(y_test, probs.transpose()[0])

    print(f"\tModel name: {model}")
    print("\t***********************************************")
    print(f"\tJaccard:    {jaccard_score(y_test, y_hat)}")
    print(f"\tPrecision:  {precision_score(y_test, y_hat)}")
    print(f"\tRecall:     {recall_score(y_test, y_hat)}")
    print(f"\tF1-score:   {f1_score(y_test, y_hat)}")
    print(f"\tLogLoss:    {log_loss(y_test, probs)}")
    print(f"\tAUC:        {auc}")
    fig, ax = plt.subplots(1, 3, figsize=(15, 4))
    plot_confusion_matrix(model, x_test, y_test, ax=ax[0])
    ax[0].set_title("Confusion Matrix")
    plot_precision_recall_curve(model, x_test, y_test, ax=ax[1])
    ax[1].set_title("Precision-Recall Curve (PRC)")
    plot_roc_curve(model, x_test, y_test, ax=ax[2])
    ax[2].plot([0, 1], [0, 1], linestyle='--')
    ax[2].set_title("ROC Curve")
    plt.show()
    
def find_best_classification_model(X, y, score='f1'):
    
    from sklearn.model_selection import RandomizedSearchCV
    """Function displays metrics of different classification models on given data set using function show_classification_model_metrics()
    Parameters:
    X - data frame of features
    y - data frame of the target feature"""
    X_train, X_test, y_train, y_test = train_test_split(X,
                                                        y,
                                                        test_size=.30,
                                                        random_state=1)

    rf_params = {
        #'bootstrap': [True],
        #'ccp_alpha': [0.0],
        #'class_weight': [None],
        'criterion': ['entropy'],  #['gini','entropy'],
        #'max_depth': [None],
        'max_features': [
            'sqrt'
        ],  #['sqrt', 'log2'], # max_features, which is the maximum number of features random forest considers to split a node
        #'max_leaf_nodes': [None],
        #'max_samples': [None],
        #'min_impurity_decrease': [0.0],
        #'min_impurity_split': [None],
        'min_samples_leaf': [
            2
        ],  #min_sample_leaf determines the minimum number of leafs required to split an internal node
        #'min_samples_split': [2],
        #'min_weight_fraction_leaf': [0.0],
        'n_estimators': [
            20, 50, 100
        ],  # n_estimators hyperparameter, which is just the number of trees the algorithm builds before taking the maximum voting or taking the averages of predictions. 
        #'n_jobs': [None],
        #'oob_score': [False],
        'random_state': [19],
        #'verbose': [0],
        #'warm_start': [False]
    }
    logr_params = {
        'C': [i*0.1 for i in range(1,100)],  #Inverse of regularization strength; default C=1 must be a positive float, smaller values specify stronger regularization.
        #         'class_weight': [None],
        #         'dual': [False],
        #         'fit_intercept': [True],
        #         'intercept_scaling': [1],
        #         'l1_ratio': [None],
        'max_iter': [100, 250, 500, 1000],
        #         'multi_class': ['auto'],
        #         'n_jobs': [None],
        'penalty': ['l2', 'l2', 'elasticnet'], #regularisation [‘l1’, ‘l2’, ‘elasticnet’]
        #         'random_state': [None],
        #         'solver': ['lbfgs'], #['newton-cg’, ‘lbfgs’, ‘liblinear’, ‘sag’, ‘saga']
        #         'tol': [0.0001],
        #         'verbose': [0],
        #         'warm_start': [False]
    }

    svc_params = {
        'C': [1.0],  #
        #         'break_ties': [False],
        #         'cache_size': [200],
        #         'class_weight': [None],
        #         'coef0': [0.0],
        #         'decision_function_shape': ['ovr'],
        #         'degree': [3], # Degree of the polynomial kernel function (‘poly’). Ignored by all other kernels.
        'gamma': [
            'scale'
        ],  # ['scale', 'auto'] Kernel coefficient for ‘rbf’, ‘poly’ and ‘sigmoid’
        'kernel': [
            'rbf', 'sigmoid', 'poly'
        ],  #['linear', 'poly', 'rbf', 'sigmoid', 'precomputed'], default='rbf'
        #         'max_iter': [-1], #Hard limit on iterations within solver, or -1 for no limit
        'probability':
        [True],  #[True, False] #Whether to enable probability estimates. 
        #         'random_state': [None],
        #         'shrinking': [True],
        #         'tol': [0.001],
        #         'verbose': [False]
    }

    cv1 = RepeatedKFold(n_splits=10, n_repeats=4, random_state=1)
    models = {
        RandomForestClassifier(): rf_params,
        LogisticRegression(): logr_params,
        SVC(): svc_params
    }
    best_models = []
    for model in models.keys():
        Grid = GridSearchCV(model, models[model], scoring=score, cv=cv1)
	#Grid = RandomizedSearchCV(model, models[model], scoring=score, cv=cv1)
        Grid.fit(X_train, y_train)
        best_model = Grid.best_estimator_
        show_classification_model_metrics(best_model, X_test, y_test)


