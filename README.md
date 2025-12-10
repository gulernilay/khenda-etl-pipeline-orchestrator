# khenda-etl-pipeline-orchestrator

Automated ETL pipelines that fetch data from multiple APIs daily and load it into SQL Server with full logging, retry, and scheduling support.

## Project Structure

- `etl/` – Transformation utilities and ETL pipeline modules
- `services/` – API clients, database connector and scheduler
- `utils/` – Logging and helper utilities
- `config/` – Centralized environment and configuration management
- `sql/` – SQL scripts for table creation and merges
- `main.py` – Application entry point

## Getting Started

1. Create a virtual environment
2. Install required packages:

pip install -r requirements.txt 3. Copy `.env.example` to `.env` and fill in values  
4. Run the application:

Bu proje, iki farklı API kaynağından günlük olarak veri toplayıp işleyerek SQL Server veritabanına yükleyen, tam ölçekli ve kurumsal seviyede tasarlanmış bir ETL (Extract–Transform–Load) otomasyon sistemidir. Mimarinin çekirdeğini oluşturan api_client_1.py ve api_client_2.py dosyaları, sırasıyla tek sayfalı ve sayfalama destekli API yapıları için özel olarak geliştirilmiş, timeout, retry, network dayanıklılığı ve JSON doğrulama gibi işlevleri içeren üretim seviyesinde HTTP istemcileridir. Transformasyon katmanında yer alan common_transforms.py modülü, kolon isimlerinin standardizasyonu, tarih formatlarının normalize edilmesi, boş veya geçersiz satırların temizlenmesi ve şema doğrulaması gibi ortak veri kalite kontrollerini merkezi şekilde sağlar. ETL’in en kritik bileşeni olan db_service.py, pyodbc tabanlı, bağlantı havuzu destekli, hızlı bulk insert için fast_executemany kullanan ve tüm tablo türleriyle çalışabilen dinamik bir veri yükleme motoru sunar; tüm hatalar güvenli şekilde ele alınır ve pipeline’a anlamlı mesajlarla iletilir. logger.py modülü ise hem dosyaya hem konsola log yazabilen, dönen log dosyalarını yöneten, timestamp ve seviyeye göre formatlanmış üretim sınıfı bir logging altyapısı sağlar ve tüm API, transform ve DB katmanlarında standart bir log davranışı sağlar. SQL tarafında, /sql klasöründeki create_tables.sql ETL için gerekli hedef tabloların kurulumunu tanımlar, indexes.sql performans için ideal indekslemeyi uygular ve merge_template.sql gelecekte incremental load gereksinimleri için kullanılabilecek MERGE yapısını sunar. Tüm bu bileşenler birlikte, izlenebilir, dayanıklı, genişletilebilir ve yüksek performanslı bir ETL sistemi oluşturur; pipeline’lar modülerdir ve her biri extract → transform → load adımlarını ayrıştırılmış şekilde yerine getirir. Böylece sistem, Chef Seasons gibi kurumsal bir yapının günlük veri işleme ihtiyaçlarını karşılayacak esneklikte ve üretim kalitesinde çalışır.
