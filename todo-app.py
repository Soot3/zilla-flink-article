import streamlit as st
from PIL import Image
from kafka import KafkaProducer,KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:1234')
consumer = KafkaConsumer('todo_topic', bootstrap_servers='localhost:1234', group_id='todo_group')

image = Image.open('./zillalogo.png')
tasks = []
st.image(image, caption='Aklivity Zilla logo')

st.header('Todo App')

with st.form("my_form", clear_on_submit=True):
   task_input = st.text_input(label='Enter a task')
   submitted = st.form_submit_button("Add Todo")
   if submitted:
       tasks.append(task_input)

       # Publish the task to Kafka
       producer.send('todo_topic', value=task_input.encode())

st.header('Todo List')
for task in tasks:
    st.write(tasks)   