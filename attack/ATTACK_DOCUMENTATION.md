Mô tả về các script tấn công bên dưới

 1. **Publish Flood Attack** (`publish_flood.py`)
**Mục đích**: Test rate limiting và khả năng xử lý high-volume traffic

**Tính năng**:
- Multi-threaded flood attack
- Random topics và payloads
- Configurable workers, messages, delay
- Variable payload sizes
- Real-time statistics

**Cách sử dụng**:
```bash
python attacks/publish_flood.py --broker localhost --port 1883 --username sensor_temp --password temp123 --workers 5 --messages 1000 --delay 10
```

**Parameters**:
- `--workers`: Số worker threads (default: 5)
- `--messages`: Số messages mỗi worker (default: 1000)
- `--delay`: Delay giữa messages (ms) (default: 0)
- `--duration`: Thời gian attack (seconds) (optional)

 2. **Wildcard Abuse Attack** (`wildcard_abuse.py`)
**Mục đích**: Test ACL wildcard filtering và topic security

**Tính năng**:
- Comprehensive wildcard patterns
- Topic hierarchy access test
- ACL restriction testing
- Message monitoring

**Wildcard Patterns**:
- `` - Global wildcard
- `+/+/+` - Three-level wildcard
- `factory/` - Factory wildcard
- `admin/` - Admin wildcard (should be blocked)
- `security/` - Security wildcard (should be blocked)

**Cách sử dụng**:
```bash
python attacks/wildcard_abuse.py --broker localhost --port 1883 --username monitor --password mon123 --workers 2 --duration 60
```

 3. **Brute-force Subscribe Attack** (`brute_force_subscribe.py`)
**Mục đích**: Test ACL enforcement và topic enumeration protection

**Tính năng**:
- Comprehensive topic generation
- Sensitive topic testing
- Authentication failure testing
- Success rate monitoring

**Target Topics**:
- Sensitive: `admin/system/config`, `security/alerts`
- Normal: `factory/tenantA/Temperature/telemetry`
- Random: Generated patterns

**Cách sử dụng**:
```bash
python attacks/brute_force_subscribe.py --broker localhost --port 1883 --username attacker --password hack123 --workers 2 --delay 100
```

 4. **Payload Anomaly Attack** (`payload_anomaly.py`)
**Mục đích**: Test payload validation và schema enforcement

**Tính năng**:
- 12 loại payload anomalies
- Malformed JSON testing
- Security injection attempts
- Binary data testing
- Unicode/Emoji attacks

**Anomaly Types**:
1. **Oversized payloads** (10KB)
2. **Malformed JSON**
3. **SQL Injection attempts**
4. **XSS attempts**
5. **Binary data**
6. **Unicode/Emoji attacks**
7. **Null bytes**
8. **Control characters**
9. **Deep nesting** (20 levels)
10. **Circular references**
11. **Invalid data types** (inf, nan)
12. **Empty/null values**

**Cách sử dụng**:
```bash
python attacks/payload_anomaly.py --broker localhost --port 1883 --username sensor_temp --password temp123 --workers 2 --delay 1000
```

 5. **Retain/QoS Abuse Attack** (`retain_qos_abuse.py`)
**Mục đích**: Test retain flag và QoS handling

**Tính năng**:
- Retain flag abuse
- QoS level testing (0, 1, 2)
- Mixed retain/QoS combinations
- Message storage testing

**Attack Types**:
- `retain`: Chỉ test retain flag
- `qos`: Chỉ test QoS levels
- `mixed`: Test cả retain và QoS

**Cách sử dụng**:
```bash
python attacks/retain_qos_abuse.py --broker localhost --port 1883 --username sensor_temp --password temp123 --type mixed --workers 2 --messages 100
```

 6. **Topic Enumeration Attack** (`topic_enumeration.py`)
**Mục đích**: Test topic discovery và ACL security

**Tính năng**:
- Comprehensive topic generation
- Topic discovery monitoring
- ACL violation detection
- Success rate analysis

**Topic Categories**:
- IoT patterns: `factory/tenantA/Temperature/telemetry`
- System topics: `system/status`, `system/config`
- Admin topics: `admin/users`, `admin/acl`
- Security topics: `security/alerts`, `security/events`
- Config topics: `config/database`, `config/network`
- Debug topics: `debug/system`, `debug/internal`

**Cách sử dụng**:
```bash
python attacks/topic_enumeration.py --broker localhost --port 1883 --username monitor --password mon123 --workers 2 --delay 200
```

 7. **Duplicate ID Attack** (`duplicate_id.py`)
**Mục đích**: Test client ID management và security

**Tính năng**:
- Duplicate client ID testing
- Sequential và simultaneous attacks
- Connection conflict testing
- Client management validation

**Attack Types**:
- `sequential`: Reconnect với cùng client ID
- `simultaneous`: Nhiều clients cùng ID đồng thời

**Cách sử dụng**:
```bash
python attacks/duplicate_id.py --broker localhost --port 1883 --username sensor_temp --password temp123 --type sequential --workers 3 --attempts 10
```

 8. **Reconnect Storm Attack** (`reconnect_storm.py`)
**Mục đích**: Test connection handling và resilience

**Tính năng**:
- Rapid reconnection testing
- Burst connection attacks
- Connection storm simulation
- Broker resilience testing

**Attack Types**:
- `storm`: Random delay reconnects
- `rapid`: Fixed interval reconnects
- `burst`: Simultaneous connection bursts

**Cách sử dụng**:
```bash
python attacks/reconnect_storm.py --broker localhost --port 1883 --username sensor_temp --password temp123 --type storm --workers 3 --reconnects 50
```
