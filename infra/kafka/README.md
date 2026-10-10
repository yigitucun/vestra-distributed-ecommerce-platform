# Vestra Kafka Güvenlik ve Kimlik Doğrulama Rehberi

Bu belge, Vestra mikroservis mimarisindeki Apache Kafka kümesinin güvenlik sıkılaştırması (SASL/SCRAM veya PLAIN kimlik doğrulaması, SSL şifreleme ve Topic ACL yetkilendirmesi) süreçlerini açıklar.

---

## 1. Mimari Güvenlik Riskleri ve Çözüm

- **Risk:** Şifresiz ve anonim (`PLAINTEXT`) Kafka broker bağlantılarında, ağ üzerindeki yetkisiz herhangi bir istemci kritik olay topic'lerine (`payment.events`, `order.events` vb.) sahte mesajlar üretebilir veya müşteri verilerini dinleyebilir.
- **Çözüm:** 
  1. Tüm mikroservislere (`order-service`, `payment-service`, `item-service`, `notification-service`) ortam değişkenleri üzerinden yönetilebilen dinamik Kafka güvenlik ve kimlik doğrulama ayarları eklendi.
  2. KRaft modundaki Kafka kümesine `StandardAuthorizer` yetkilendirme sınıfı entegre edildi.
  3. Servis bazlı en az yetki prensibiyle (Least Privilege) çalışan topic ACL tanımları hazırlandı.

---

## 2. Mikroservis Ortam Değişkenleri (Spring Boot)

Her mikroservisin `application.yaml` dosyasında aşağıdaki parametreler desteklenmektedir:

| Değişken | Varsayılan | Açıklama |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker adresleri (Docker içi: `kafka:29092`) |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | İletişim protokolü (`PLAINTEXT`, `SASL_PLAINTEXT`, `SASL_SSL`, `SSL`) |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | SASL mekanizması (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) |
| `KAFKA_SASL_JAAS_CONFIG` | *(boş)* | JAAS oturum açma yapılandırması |

### SASL/SCRAM Örnek Konfigürasyonu:
```env
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required username="order-service" password="secure-password-here";
```

---

## 3. KRaft Authorizer ve Topic ACL Yetkileri

Kafka broker'da ACL'leri zorunlu kılmak için `infra/docker-compose.yml` içinde:
```yaml
KAFKA_AUTHORIZER_CLASS_NAME: org.apache.kafka.metadata.authorizer.StandardAuthorizer
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: 'false'
KAFKA_SUPER_USERS: 'User:admin'
```
ayarlandığında, topic erişimleri yalnızca tanımlı kurallarla gerçekleşir.

Yetkileri uygulamak için `infra/kafka/setup-acls.sh` betiği çalıştırılabilir:
- **Debezium Outbox:** `order.events`, `payment.events`, `item.events`, `user.events`, `product.events` topic'lerine yalnızca yazma (`Write`) hakkına sahiptir.
- **Order Service:** `payment.events` ve `item.events` topic'lerinden okuma (`Read`) hakkına sahiptir.
- **Item Service:** `order.events` ve `product.events` topic'lerinden okuma (`Read`) hakkına sahiptir.
- **Notification Service:** `user.events` ve `order.events` topic'lerinden okuma (`Read`) hakkına sahiptir.
