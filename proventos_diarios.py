# alerts/proventos_diarios.py
from datetime import date, timedelta

def main():
    hoje = date.today()
    amanha = hoje + timedelta(days=1)

    print("🔔 ROBÔ DE PROVENTOS — TESTE")
    print(f"Hoje: {hoje}")
    print(f"Amanhã: {amanha}")
    print("Status: robô executado com sucesso.")

if __name__ == "__main__":
    main()
