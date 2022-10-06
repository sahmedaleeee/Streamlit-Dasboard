# ----import------
import gspread as gs
import requests as r
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image

# -----page setting-----
logo = Image.open("logo.png")
st.set_page_config(
    page_title="CFBP",
    page_icon=logo,
    layout="wide"
)
st.get_option("theme.primaryColor")
st.get_option("theme.textColor")
st.get_option("theme.secondaryBackgroundColor")
st.get_option("theme.font")


# ----- functions -----
def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

def get_states():
    states= r.get("https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json")
    return states.json()

def load_googlesheet_data():
    gc = gs.service_account(filename='credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/11PSAUcDzUFQYhh6nJC_40decAutgaE2FHWIkXaLMFSQ/edit?usp=sharing')
    ws = sh.worksheet('Data')
    dataframe = pd.DataFrame(ws.get_all_records())
    return dataframe

def percentage(num_a, num_b):
    pct = round((num_a / num_b) * 100,2)
    return f"{pct}%"

def year_month_number(ym):
    months = {
        'jan': '01',
        'feb': '02',
        'mar': '03',
        'apr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'aug': '08',
        'sep': '09',
        'oct': '10',
        'nov': '11',
        'dec': '12'
    }
    a = ym.strip()[:3].lower()
    b = ym.strip()[4:8]
    ez = months[a]
    output = int(f"{b}{ez}")
    return output

if check_password():
    # ----- data tranformation ------
    dataframe = load_googlesheet_data()
    states_full_name = get_states()
    df=dataframe.replace({"state":states_full_name})


    # ---- get unique values ------
    date_list = df['date_received'].unique().tolist()
    date_list_new=date_list[:]
    date_list_new.insert(0,'All')

    states_list = df['state'].unique().tolist()
    states_list_new = states_list[:]
    states_list_new.insert(0,'All')

    product_list = df['product'].unique().tolist()
    product_list_new = product_list[:]
    product_list_new.insert(0,'All')

    submitted_via_list = df['submitted_via'].unique().tolist()
    submitted_via_list_new = submitted_via_list[:]
    submitted_via_list_new.insert(0,'All')

    issue_list = df['issue'].unique().tolist()
    issue_list_new = issue_list[:]
    issue_list_new.insert(0,'All')

    timely_list = df['timely'].unique().tolist()
    timely_list_new = timely_list[:]
    timely_list_new.insert(0,'All')

    response_list = df['company_response'].unique().tolist()
    response_list_new = response_list[:]
    response_list_new.insert(0,'All')


    # ----- defining default values ------
    default_dates = date_list_new.index("All")
    default_state = states_list_new.index("All")
    default_product = product_list_new.index("All")
    default_submmitted_via = submitted_via_list_new.index("All")
    default_issue = issue_list_new.index("All")
    default_timely = timely_list_new.index("All")
    default_response = response_list_new.index("All")


    # ----- create Filters Pane------
    st.sidebar.header("Filters")

    date_filter = st.sidebar.selectbox("Date", date_list_new,index=default_dates)
    if "All" in date_filter:
        date_filter = date_list
    else:
        date_filter

    state_filter=st.sidebar.selectbox("State", states_list_new,index=default_state)
    if "All" in state_filter:
        state_filter = states_list
    else: 
        state_filter

    product_filter=st.sidebar.selectbox("Product", product_list_new,index=default_product)
    if "All" in product_filter:
        product_filter = product_list
    else: 
        product_filter

    submitted_via_filter=st.sidebar.selectbox("Submitted Via", submitted_via_list_new,index=default_submmitted_via)
    if "All" in submitted_via_filter:
        submitted_via_filter =submitted_via_list
    else: 
        submitted_via_filter

    issue_filter=st.sidebar.selectbox("Issue", issue_list_new,index=default_issue)
    if "All" in issue_filter:
        issue_filter = issue_list
    else: 
        issue_filter

    timely_filter=st.sidebar.selectbox("Timely", timely_list_new,index=default_timely)
    if "All" in timely_filter:
        timely_filter = timely_list
    else: 
        timely_filter

    response_filter=st.sidebar.selectbox("Respone", response_list_new,index=default_response)
    if "All" in response_filter:
        response_filter = response_list
    else: 
        response_filter


    df_filtered = df.query(
        "state == @state_filter & date_received == @date_filter & product == @product_filter & submitted_via == @submitted_via_filter & issue == @issue_filter & timely == @timely_filter & company_response == @response_filter"
    )


    #----- title -----
    st.title(":chart_with_upwards_trend: CFPB Dashboard")


    # ----- defining KPIs---------
    total_complaints = df_filtered[df_filtered.columns[0]].count()
    avg_complaints_per_month = int(total_complaints/len(date_list))
    complaint_closed = len(df_filtered[df_filtered['company_response'].str.contains('Closed')]) 
    complaint_in_progress = len(df_filtered[df_filtered['company_response'].str.contains('In progress')]) 
    timely_complaint = percentage(len(df_filtered[df_filtered['timely'].str.contains('Yes')]),total_complaints)
    month = (df_filtered.groupby(['date_received'])['date_received'].count().reset_index(name='count').sort_values(['count'], ascending=False).head(1).values[0].tolist())[0]

    c1,c2,c3,c4,c5,c6 = st.columns(6)

    c1.metric("Number of Complaints",total_complaints)
    c2.metric("Avg Complaints",avg_complaints_per_month)
    c3.metric("Closed Complaints",complaint_closed)
    c4.metric("In Progress",complaint_in_progress)
    c5.metric("Timely",timely_complaint)
    c6.metric("Month",month)
    st.markdown("---")


    # ------ Configuration of metrics ------
    st.markdown('''
    <style>
    /*center metric label*/
    [data-testid="stMetricLabel"] > div:nth-child(1) {
        justify-content: center;
    }

    /*center metric value*/
    [data-testid="stMetricValue"] > div:nth-child(1) {
        justify-content: center;
    }
    </style>
    ''', unsafe_allow_html=True)

    st.markdown("""
    <style>
    div[data-testid="metric-container"] {
    background-color: #262730;
    border: 1px solid #2B2D32;
    padding: 5% 5% 5% 10%;
    border-radius: 5px;
    overflow-wrap: break-word;
    }

    /* breakline for metric text         */
    div[data-testid="metric-container"] > label[data-testid="stMetricLabel"] > div {
    overflow-wrap: break-word;
    white-space: break-spaces;
    color: #BCBFC4;
    }

    </style>
    """
    , unsafe_allow_html=True)


    # ----- navigation -------
    selected = option_menu(
            menu_title= None,
            options= ['Product','Year Month','Submitted-Via','Issues & Sub-Issues','Data'],
            icons= ['credit-card-2-front','calendar-check','box-arrow-up','boxes','device-ssd'],
            orientation= "horizontal",
            styles={
                "container": {"padding": "0.5"},
                "icon": {"color": "#ebe6dc"}, 
                "nav-link": { "text-align": "center", "margin":"5px", "--hover-color": "#efaaa4"},
            }
    )
    st.markdown("---")

    #----Bar chart-----
    complaints_by_products = df_filtered.groupby(['product'])['product'].count().reset_index(name='count').sort_values(['count'], ascending=False)
    complaints_by_products['color']='#E1E1E1'
    complaints_by_products['color'][0:1]='#ff4b4b'

    fig_product_complaint = px.bar(
        complaints_by_products,
        y = 'count',
        x = 'product',
        orientation = "v",
        title = "<b>Number of Complaints by Product</b>",
        labels={
            'product' : 'Product',
            'count' : 'Number of Complaints'
        },
        color='color',
        color_discrete_sequence=complaints_by_products.color.unique(),
        template="plotly_white",
        height=700,
        log_y= True
    )

    fig_product_complaint.update_layout(
        plot_bgcolor="#262730",
        showlegend=False,
        xaxis= (dict(showgrid=False, showticklabels=False)),
        yaxis= (dict(showgrid=False, visible = True, showticklabels=False)),
        title= (dict(x= 0.5, xanchor= "center",font_size= 22, font_color= 'Grey'))
    )

    fig_product_complaint.update_traces(
        hovertemplate="<br> Number of Complaints: <b>%{value}</b> </br> <br> Product: <b>%{label}</b> </br> "
    )


    # ---- line chart -----
    complaints_by_year_month = df_filtered.groupby(['date_received'])['date_received'].count().reset_index(name='count')
    x = complaints_by_year_month['date_received'].tolist()
    year_month_n = []

    for _date in x:
        ymn = year_month_number(_date) # fuction created to get yyyymm
        year_month_n.append(ymn)

    complaints_by_year_month['year_month_sort'] = year_month_n
    complaints_by_year_month_sorted= complaints_by_year_month.sort_values(by=['year_month_sort'], ascending=True,ignore_index=True)

    fig_complaints_year_month= px.area(
        complaints_by_year_month_sorted,
        x='date_received',
        y='count',    
        title = "<b>Number of Complaints by Year Month</b>",
        markers=True,
        labels={
            'date_received' : 'Year Month',
            'count' : 'Number of Complaints'
        },
        custom_data = ['date_received']
    )

    fig_complaints_year_month.update_layout(
        plot_bgcolor="#262730",
        xaxis=(dict(showgrid=True, gridcolor='#2B2D32',showticklabels=True)),
        yaxis=(dict(showgrid=True, gridcolor='#2B2D32', visible=True ,showticklabels=True)),
        title= (dict(x= 0.5, xanchor= "center",font_size= 22, font_color= 'Grey'))
    )

    fig_complaints_year_month.update_traces(
        line=dict(color='#ff4b4b',width=2), 
        marker = dict(color= '#E1E1E1',size=6),
        hovertemplate="<b>%{customdata}</b> has <b>%{y}</b> complaints "
    )

    # ----pie chart-----
    complaints_by_submit_via= (df_filtered.groupby(['submitted_via'])['submitted_via'].count().reset_index(name='count').sort_values(['count'], ascending=False))
    sv= len(complaints_by_submit_via)
    color_code = ['#ff4b4b','#3C3C3C','#A39594','#CEC4C2','#DFDFDF']
    complaints_by_submit_via['color']=color_code[0:sv]

    fig_complaints_submit_via = px.pie(
        complaints_by_submit_via,
        names = 'submitted_via',
        values='count',
        title="<b>Number of Complaints by Submitted_via<b/>",
        color='color',
        color_discrete_sequence=complaints_by_submit_via.color.unique(),
        hole=0.7,
        custom_data= ['submitted_via']
    )

    fig_complaints_submit_via.update_layout(
    plot_bgcolor = "#262730",
    legend = dict(bgcolor ="#262730"),
    title= (dict(x= 0.5, xanchor= "center",font_size= 22, font_color= 'Grey'))
    )

    fig_complaints_submit_via.update_traces(
        hovertemplate = "<b>%{value}</b> complaints through <b>%{customdata} </b>"
    )


    #----- treemap-----
    complaints_by_issues= (df_filtered.groupby(['issue','sub_issue']).size().reset_index(name='count').sort_values(['count'], ascending=False))
    fig_complaints_by_issues = px.treemap(
        complaints_by_issues,
        path= ['issue', 'sub_issue'],
        values= 'count',
        title = "<b>Number of Complaints by Issues & Sub-Issues</b>",
        color='count',
        color_continuous_scale='OrRd_r'
    )

    fig_complaints_by_issues.update_layout(
        plot_bgcolor="#262730",
        title= (dict(x= 0.5, xanchor= "center",font_size= 22, font_color= 'Grey'))
    )   

    fig_complaints_by_issues.update_traces(
        hovertemplate = "<br>Issue: <b>%{label}</b></br> <br>Sub-Issue: <b>%{parent}</b></br> <br>Complaints: <b>%{value}</b> </br>"
    )

    if selected =="Product":
        st.plotly_chart(
            fig_product_complaint,
            use_container_width=True
        )

    if selected =="Year Month":
        st.plotly_chart(
            fig_complaints_year_month,
            use_container_width=True
        )

    if selected == 'Submitted-Via':
        st.plotly_chart(
            fig_complaints_submit_via,
            use_container_width=True
        )

    if selected == 'Issues & Sub-Issues':
        st.plotly_chart(
            fig_complaints_by_issues,
            use_container_width=True
        )

    if selected == 'Data':
        st.write(df_filtered.head(10))

    st.markdown("---")
