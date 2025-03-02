from flask import Flask, render_template, request
import pandas as pd
import json
import os
import matplotlib.pyplot as plt
from io import BytesIO
import base64

app = Flask(__name__)

def load_data():
    all_files = [pos_json for pos_json in os.listdir("data")
                 if pos_json.startswith("Streaming_History") and pos_json.endswith(".json")]
    
    dfs = []
    for f_name in all_files:
        with open(os.path.join("data", f_name), encoding="utf-8") as f:
            json_data = pd.json_normalize(json.loads(f.read()))
            dfs.append(json_data)
    
    if dfs:
        df = pd.concat(dfs, sort=False)
        df['endTime'] = pd.to_datetime(df['ts'])
        return df
    return None

def generate_plot(start_date=None, end_date=None):
    df = load_data()
    if df is None:
        return None
    
    if start_date and end_date:
        mask = (df['endTime'] >= start_date) & (df['endTime'] <= end_date)
        df = df.loc[mask]
    
    df_time = df.groupby(df['endTime'].dt.date).agg({'ms_played': 'sum'})
    df_time['hrPlayed'] = df_time['ms_played'] / (1000 * 60 * 60)
    
    fig, ax = plt.subplots()
    ax.plot(df_time.index, df_time['hrPlayed'], marker='o')
    ax.set_title("Spotify Listening Time per Day")
    ax.set_ylabel("Hours Played")
    ax.set_xlabel("Date")
    fig.autofmt_xdate()
    
    img = BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode()
    return plot_url

@app.route('/', methods=['GET', 'POST'])
def home():
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    plot_url = generate_plot(start_date, end_date) if start_date and end_date else generate_plot()
    return render_template('index.html', plot_url=plot_url)

if __name__ == '__main__':
    app.run(debug=True)
