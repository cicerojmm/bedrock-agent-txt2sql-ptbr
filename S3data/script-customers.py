import psycopg2
import csv

# Função para criar a tabela no banco de dados
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS Customers (
                        Cust_Id TEXT,
                        Customer TEXT,
                        Balance TEXT,
                        Past_Due TEXT,
                        Vip TEXT
                    )''')
    conn.commit()

# Função para inserir dados na tabela
def insert_data(conn, data):
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO Customers (Cust_Id, Customer, Balance, Past_Due, Vip) VALUES (%s, %s, %s, %s, %s)', data)
    conn.commit()

# Função para ler dados de um arquivo CSV
def read_csv(file_path):
    with open(file_path, 'r', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Pula o cabeçalho
        return [tuple(row) for row in csvreader]

# Função principal
def main():
    # Informações de conexão com o banco de dados PostgreSQL
    dbname = 'medical_procedures'
    user = 'postgres'
    password = 'Genai2024'
    host = 'database-1.cykfubzsemsi.us-east-1.rds.amazonaws.com'  # Por exemplo, localhost
    port = '5432'  # Por exemplo, 5432

    # Conecta ao banco de dados
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    # Cria a tabela se ainda não existir
    create_table(conn)

    # Lê os dados do arquivo CSV
    csv_file_path = 'mock-data-customers.csv'
    data = read_csv(csv_file_path)

    # Insere os dados na tabela
    insert_data(conn, data)

    # Fecha a conexão com o banco de dados
    conn.close()

if __name__ == "__main__":
    main()
