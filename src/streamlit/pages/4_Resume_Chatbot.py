import os
import streamlit as st
import random
import time
from typing import Dict
# from dotenv import load_dotenv
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content
from agents import Agent, Runner, trace, function_tool
import asyncio

# load_dotenv(override=True)

st.write("This is a sample agent which allows you to chat with my resume.")
st.caption("Note that this is just a demo.")

openai_api_key = os.getenv('OPENAI_API_KEY')

@function_tool
def record_user_details(subject: str, html_body: str) -> Dict[str, str]:
    """ Send out an email with the given subject and HTML body to contact e-mail"""
    sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
    from_email = Email(os.environ.get('FROM_EMAIL_SENDGRID'))
    to_email = To(os.environ.get('TO_EMAIL_SENDGRID'))
    content = Content("text/html", html_body)
    mail = Mail(from_email, to_email, subject, content).get()
    sg.client.mail.send.post(request_body=mail)
    return {"status": "success"}

name = "Miguel Guzman"

system_prompt = f"You are acting as {name}. You are answering questions on {name}'s website, \
particularly questions related to {name}'s career, background, skills and experience. \
Your responsibility is to represent {name} for interactions on the website as faithfully as possible. \
You are given a summary of {name}'s background and LinkedIn profile which you can use to answer questions. \
Be professional and engaging, as if talking to a potential client or future employer who came across the website. \
If you don't know the answer to any question, say it directly, even if it's about something trivial or unrelated to career. \
If the user is engaging in discussion, try to steer them towards getting in touch via email; ask for their email and record it using your record_user_details tool. "

summary = f"""
MIGUEL ANTONIO GUZMAN TOBAR
Name: Miguel Antonio Guzmán Tobar
Role: Data Engineer
Location: Spain (GMT+1)
Languages: Spanish (Native), English (Fluent)
Experience: ~7+ years in Data Engineering, ETL, Data Warehousing, Distributed Processing, Cloud.
Contact Style: Clear, technical, practical. Focus on delivering working solutions and explaining trade-offs.
Strengths: ETL optimization, data modeling, real-time pipelines, migration projects, cloud platforms, debugging production systems.
CORE TECH SKILLS
Languages: Python (Advanced), SQL (Advanced), Scala (Intermediate), Java (Intermediate), JavaScript (Advanced), PHP (Intermediate)
Data & Processing: Apache Spark (Advanced), PySpark, DBT/Dataform, Airflow, AWS Glue, Snowflake, Redshift, BigQuery
Cloud: AWS (Advanced), Azure, GCP
Streaming: Kinesis (Data Streams / Firehose / Analytics)
Databases: PostgreSQL, MySQL, SQL Server, Elasticsearch
Data Modeling & Warehousing: Dimensional modeling, lakehouse architectures, OLTP/OLAP design
Other: Docker, Terraform/CloudFormation, Tableau, WordPress/WooCommerce integrations (past experience)
EXPERIENCE SUMMARY (COMPRESSED)
Hiberus (2024-Present) - Data Engineer
Built real-time AWS pipeline (Kinesis → Glue → Redshift), reducing processing time by 90%.
Migrated ETLs from Azure Synapse/ADF → Azure Databricks (80% faster pipelines, +20% data accuracy).
Supported existing Azure & Airflow pipelines.
Migrated ETLs from Azure to Google Cloud (Dataform + BigQuery + Airflow).
Leapfin (2022-2024) - Data Engineer
Migrated enterprise customers to new pipelines → 50% faster ETL runtimes.
Designed async S3 → Elasticsearch chunked uploader → 30% faster ingestion, 90% fewer errors.
On-call rotations for production pipelines (AWS + Airflow).
Integrated customer APIs and maintained data schemas & Tableau dashboards.
Tecoloco / Saongroup (2021-2022) - Big Data Engineer
Built ETL pipelines w/ Scala + Spark + Airflow → Redshift/S3/Hadoop.
Added data quality checks → +10% accuracy across warehouses.
Applaudo Studios (2020-2021) - Big Data Engineer (Client: Walmart)
Spark execution optimization → 90% faster financial reconciliation jobs.
Managed ETLs on Azure/GCP, produced BigQuery & MySQL datasets for Tableau reporting.
Clobi Tech (2019-2020) - IT Consultant
Led 3 developers; built web scraping + sentiment analysis pipeline → +20% client base.
Built e-commerce solutions & payment gateway integrations.
Managed AWS servers, security, and BI dashboards (Pentaho).
Fomilenio II (2018-2019) - IT Consultant
Automated government import/export authorization workflows → reduced processing from 27 days to 1 minute via REST service integration.
Clearview Live (2017-2018) - ETL Developer
Integrated ACD + CRM systems into custom BI dashboards.
Built staging schemas + stored procedures to feed DW.
PROJECT HIGHLIGHT CAPABILITIES
Real-time ingestion pipelines
Cloud-native ETL modernization & migration
Data quality automation
Performance optimization of Spark jobs
Financial data modeling pipelines
Dashboard / metric pipeline support (Tableau, custom solutions)
EDUCATION (ESSENTIALS ONLY)
Udacity: Data Scientist Nanodegree (2024), Cloud DW, Data Lakes w/ Spark+Databricks (2024)
University of Puerto Rico: Data Science Certificate (2019)
Coursera (Stanford): Machine Learning (2019)
B.S. Computer Systems Engineering (2018)
"""
linkedin = "https://www.linkedin.com/in/miguel-antonio-g-659815b9/"
system_prompt += f"\n\n## Summary:\n{summary}\n\n## LinkedIn Profile:\n{linkedin}\n\n"
system_prompt += f"With this context, please chat with the user, always staying in character as {name}."

resume_agent = Agent(
    name="Resume agent",
    instructions=system_prompt,
    tools=[record_user_details],
    model="gpt-4o-mini"
)

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Let's start chatting! 👇"}]

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("What is up?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""

        result = Runner.run_sync(
            starting_agent=resume_agent,
            input=prompt
        )

        # El SDK expone un helper para obtener el texto de salida
        assistant_response = result.final_output

        # record_user_details("This is a test", prompt)
        # Simulate stream of response with milliseconds delay
        for chunk in assistant_response.split():
            full_response += chunk + " "
            time.sleep(0.05)
            # Add a blinking cursor to simulate typing
            message_placeholder.markdown(full_response + "▌")
        message_placeholder.markdown(full_response)
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": full_response})