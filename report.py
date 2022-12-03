import datetime
import telebot
import firebirdsql
from sshtunnel import SSHTunnelForwarder
import pandas as pd
from io import BytesIO


# Create bot
bot = telebot.TeleBot(token='1173390946:AAFd65-o35punvwx7NXT92HtR_-o5a9aTms')

# Get data from database
server = SSHTunnelForwarder(
            ('mail.pentada-brok.com', 57300),
            ssh_username="agmorev",
            ssh_password="Jur48dl§hfi!83",
            remote_bind_address=('192.168.70.99', 3051),
            local_bind_address=('127.0.0.1', 53051)
        )

server.start()

print(server)

conn = firebirdsql.connect(
        host='127.0.0.1',
        database='C:\MasterD\MDGarant\PentadaDB\Db\MDGARANT.FDB',
        port=53051,
        user='SYSDBA',
        password='masterkey',
        charset='UTF8'
)


# REPORT ABOUT QUANTITY AND SUM OF YESTERDAY'S WARRANTIES

yesterday = datetime.date.today() - datetime.timedelta(days=1)
month = yesterday.month
year = yesterday.year

df = pd.read_sql("SELECT GL_DATE_CR, GL_CL_NAME, GL_CL_OKPO, GL_SUMMA, GL_PR FROM GARANT_LIST WHERE GL_DATE_CR>'01.{}.{} 00:00' AND GL_DATE_CR<'{} 23:59' AND GL_PR!=2;".format(month, year, yesterday), conn)

pd.set_option('display.float_format', lambda x: '%.2f' % x)
print(df)
total_number = df.shape[0]
print('Загальна кількість', total_number)
total_sum = "{:,.2f}".format(df['GL_SUMMA'].sum())
print('Загальна сума', total_sum)
df['COUNT'] = 1
print('--------------------------')
df1 = df[(df['GL_DATE_CR'] > yesterday.strftime("%m-%d-%Y 00:00")) & (df['GL_DATE_CR'] < yesterday.strftime("%m-%d-%Y 23:59"))]
print(df1)
day_number = df1.shape[0]
print('Кількість', day_number)
day_sum = "{:,.2f}".format(df1['GL_SUMMA'].sum())
print('Сума', day_sum)
df1['COUNT'] = 1
print('--------------------------')
final_table = df.groupby('GL_CL_NAME').agg({'COUNT': 'count', 'GL_SUMMA': 'sum'}).reset_index(drop=False)
final_table1 = df1.groupby('GL_CL_NAME').agg({'COUNT': 'count', 'GL_SUMMA': 'sum'}).reset_index(drop=False)
print(final_table)
print('--------------------')
print(final_table1)
month_table = final_table.rename(columns={'GL_CL_NAME': 'Клієнт', 'COUNT': 'Кількість', 'GL_SUMMA': 'Сума'})
yesterday_table = final_table1.rename(columns={'GL_CL_NAME': 'Клієнт', 'COUNT': 'Кількість', 'GL_SUMMA': 'Сума'})
table1 = yesterday_table.sort_values(by=['Кількість'], ascending=False).to_string(header=False, columns=['Клієнт', 'Кількість'], sparsify=False, index=False, max_colwidth=15, col_space=10, justify='left', formatters={"Сума": "{:,.2f}".format})
table2 = yesterday_table.sort_values(by=['Сума'], ascending=False).to_string(header=False, columns=['Клієнт', 'Сума'], sparsify=False, index=False, max_colwidth=15, col_space=10, justify='left', formatters={"Сума": "{:,.2f}".format})

# Send message to group
#bot.send_message(-1001206691663, '📊З початку місяця оформлено фінансових гарантій:\n-кількість - *{}* ГД\n-сума - *{}* грн.'.format(total_number, total_sum), parse_mode='Markdown').json
#bot.send_message(-1001206691663, '📊За минулу добу *{}* оформлено фінансових гарантій:\n-кількість - *{}* ГД\n-сума - *{}* грн.'.format(yesterday.strftime("%d.%m.%Y"), day_number, day_sum), parse_mode='Markdown').json
#bot.send_message(-1001206691663, '🔝*За кількістю:*\n{}'.format(table1), parse_mode='Markdown').json
#bot.send_message(-1001206691663, '🔝*За сумою:*\n{}'.format(table2), parse_mode='Markdown').json

# Pikovskiy
#bot.send_message(119717130, '📊З початку місяця оформлено фінансових гарантій:\n-кількість - *{}* ГД\n-сума - *{}* грн.'.format(total_number, total_sum), parse_mode='Markdown').json
#bot.send_message(119717130, '📊За минулу добу *{}* оформлено фінансових гарантій:\n-кількість - *{}* ГД\n-сума - *{}* грн.'.format(yesterday.strftime("%d.%m.%Y"), day_number, day_sum), parse_mode='Markdown').json
#bot.send_message(119717130, '🔝*За кількістю:*\n{}'.format(table1), parse_mode='Markdown').json
#bot.send_message(119717130, '🔝*За сумою:*\n{}'.format(table2), parse_mode='Markdown').json

# Morev
bot.send_message(1061732281, '📊З початку місяця оформлено фінансових гарантій:\n-кількість - *{}* ГД\n-сума - *{}* грн.'.format(total_number, total_sum), parse_mode='Markdown').json
bot.send_message(1061732281, '📊За минулу добу *{}* оформлено фінансових гарантій:\n-кількість - *{}* ГД\n-сума - *{}* грн.'.format(yesterday.strftime("%d.%m.%Y"), day_number, day_sum), parse_mode='Markdown').json
bot.send_message(1061732281, '🔝*За кількістю:*\n{}'.format(table1), parse_mode='Markdown').json
bot.send_message(1061732281, '🔝*За сумою:*\n{}'.format(table2), parse_mode='Markdown').json

# REPORT ABOUT EXPIRATION DATE OF WARRANTIES

exp_date = (datetime.datetime.today() + datetime.timedelta(days=3)).strftime("%Y-%m-%d")
df1 = pd.read_sql('''
    SELECT
        GL_NUM,
        GL_DATE_CR,
        GL_CL_NAME,
        GL_CL_OKPO,
        GL_CAR_NAME,
        GL_CAR_ADR,
        GL_CCD_07_01,
        GL_CCD_07_02,
        GL_CCD_07_03,
        GL_CCD_DATE,
        GG_33_01,
        GG_31_01,
        GG_35_01,
        GL_SUMMA,
        GL_PR,
        GL_DATE_EXP
    FROM GARANT_LIST
    INNER JOIN GARANT_LIST_GOODS
    ON GARANT_LIST.IDENT=GARANT_LIST_GOODS.IDENT
    WHERE (GL_PR=1 OR GL_PR=3) AND (GL_DATE_EXP<='{}');
    '''.format(exp_date), conn)

if len(df1) > 0:
    # Send message to group
    #bot.send_message(-1001206691663, '‼️‼️‼️ *УВАГА* ‼️‼️‼️', parse_mode='Markdown')

    # Morev
    bot.send_message(1061732281, '‼️‼️‼️ *УВАГА* ‼️‼️‼️', parse_mode='Markdown')

    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    df1.fillna("-", inplace=True)
    df1['GL_DATE_CR'].replace({'30.12.1899 00:00': ''}, inplace=True)
    df1['GL_CCD_DATE'].replace({'30.12.1899 00:00': ''}, inplace=True)
    df1['GL_DATE_EXP'].replace({'30.12.1899 00:00': ''}, inplace=True)

    df1 = df1.groupby('GL_NUM').agg({
        'GL_DATE_CR': 'first',
        'GL_CL_NAME': 'first',
        'GL_CL_OKPO': 'first',
        'GL_CAR_NAME': 'first',
        'GL_CAR_ADR': 'first',
        'GL_CCD_07_01': 'first',
        'GL_CCD_07_02': 'first',
        'GL_CCD_07_03': 'first',
        'GL_CCD_DATE': 'first',
        'GG_33_01': ', '.join,
        'GG_31_01': '; '.join,
        'GG_35_01': sum,
        'GL_SUMMA': 'first',
        'GL_PR': 'first',
        'GL_DATE_EXP': 'first'
    })


    total_number = df1.shape[0]
    total_sum = "{:,.2f}".format(df1['GL_SUMMA'].sum())

    # Send message to group
    #bot.send_message(-1001206691663, '⌛️ За 3 дні закінчується термін дії для фінансових гарантій:\n-кількість - *{}* ГД\n-сума - *{}* грн.'.format(total_number, total_sum), parse_mode='Markdown')

    # Morev
    bot.send_message(1061732281, '⌛️ За 3 дні закінчується термін дії для фінансових гарантій:\n-кількість - *{}* ГД\n-сума - *{}* грн.'.format(total_number, total_sum), parse_mode='Markdown')

    #FORM XLSX TABLE AND SEND TO USER
    in_memory = BytesIO()
    in_memory.name = 'report_expire.xlsx'
    df1.to_excel(in_memory)
    in_memory.seek(0,0)
    # Send message to group
    #bot.send_document(-1001206691663, in_memory)

    # Morev
    bot.send_document(1061732281, in_memory)

    # Send message to group
    #bot.send_message(-1001206691663, "‼️ Необхідно з'ясувати причини затримки в доставленні вантажу у імпортера, експортера, перевізника та доповісти керівництву", parse_mode='Markdown')

    # Morev
    bot.send_message(1061732281, "‼️ Необхідно з'ясувати причини затримки в доставленні вантажу у імпортера, експортера, перевізника та доповісти керівництву", parse_mode='Markdown')
else:
    pass

conn.close()
server.stop()