import psycopg2
import csv

# Função para criar a tabela no banco de dados
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS Procedures (
                        Procedure_Id TEXT,
                        Procedure TEXT,
                        Category TEXT,
                        Price TEXT,
                        Duration_Minutes TEXT,
                        Insurance_Covered TEXT,
                        Customer_Id TEXT
                    )''')
    conn.commit()

# Função para inserir dados na tabela
def insert_data(conn, data):
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO Procedures (Procedure_Id, Procedure, Category, Price, Duration_Minutes, Insurance_Covered, Customer_Id) VALUES (%s, %s, %s, %s, %s, %s, %s)', data)
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
    user = 'datahandsonmds'
    password = '0560Zf#q^4^hq3pc'
    host = 'transactional.cluster-cihooyesuplm.us-east-1.rds.amazonaws.com'  # Por exemplo, localhost
    port = '5432'  # Por exemplo, 5432

    # Conecta ao banco de dados
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    # Cria a tabela se ainda não existir
    create_table(conn)

    # Lê os dados do arquivo CSV
    csv_file_path = 'mock-data-procedures.csv'
    data = read_csv(csv_file_path)

    # Insere os dados na tabela
    insert_data(conn, data)

    # Fecha a conexão com o banco de dados
    conn.close()

if __name__ == "__main__":
    main()
