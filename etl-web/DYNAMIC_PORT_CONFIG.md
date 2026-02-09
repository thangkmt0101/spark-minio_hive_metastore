# Dynamic Port Configuration

Hướng dẫn cấu hình port động từ environment variable.

## 📋 Tổng quan

Port configuration giờ đây hoàn toàn dynamic:
- ✅ Đọc từ file `.env`
- ✅ Có thể override bằng environment variable
- ✅ Health check tự động sử dụng đúng port
- ✅ Port mapping Docker tự động adapt

## 🔧 Configuration Files

### 1. `.env` File

```env
# Application Port (có thể thay đổi)
APP_PORT=5333
```

### 2. Dockerfile

```dockerfile
# Set environment variable với default value
ENV APP_PORT=5333

# Expose port (dynamic)
EXPOSE ${APP_PORT}

# Health check (sử dụng env var)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import os, requests; requests.get(f'http://localhost:{os.getenv(\"APP_PORT\", \"5333\")}')"
```

### 3. docker-compose.yml

```yaml
web:
  environment:
    APP_PORT: ${APP_PORT:-5333}  # Từ .env hoặc default 5333
  ports:
    - "${APP_PORT:-5333}:${APP_PORT:-5333}"  # host:container đều dynamic
```

### 4. app.py

```python
if __name__ == '__main__':
    port = int(os.getenv('APP_PORT'))
    app.run(host='0.0.0.0', port=port)
```

## 🚀 Cách sử dụng

### 1. Sử dụng port mặc định (5333)

```bash
# .env
APP_PORT=5333

# Start
docker-compose up -d

# Access
http://localhost:5333
```

### 2. Thay đổi port trong .env

```bash
# Sửa .env
APP_PORT=8080

# Rebuild và restart
docker-compose up -d --build

# Access
http://localhost:8080
```

### 3. Override port khi chạy

```bash
# Override với environment variable
APP_PORT=9000 docker-compose up -d

# Access
http://localhost:9000
```

### 4. Multiple instances (khác port)

```bash
# Instance 1
APP_PORT=5333 docker-compose up -d

# Instance 2 (file docker-compose khác hoặc override)
APP_PORT=5334 docker-compose -f docker-compose.yml -p etl2 up -d
```

## 📝 Port Flow

```
.env file
   ↓
APP_PORT=5333
   ↓
docker-compose.yml
   ↓
environment: APP_PORT=5333
   ↓
Dockerfile
   ↓
ENV APP_PORT=5333
   ↓
app.py
   ↓
Flask runs on port 5333
   ↓
Health check uses port 5333
```

## 🔍 Verify Port Configuration

### 1. Check environment variable trong container

```bash
docker-compose exec web env | grep APP_PORT
# Output: APP_PORT=5333
```

### 2. Check Flask listening port

```bash
docker-compose logs web | grep "Running on"
# Output: * Running on http://0.0.0.0:5333
```

### 3. Check port mapping

```bash
docker-compose ps
# Output:
# NAME      PORTS
# etl-web   0.0.0.0:5333->5333/tcp
```

### 4. Check health check

```bash
docker inspect etl-web --format='{{.State.Health.Status}}'
# Output: healthy
```

### 5. Test connectivity

```bash
curl http://localhost:5333
# Should return HTML of index page
```

## 💡 Use Cases

### Dev vs Production

**Development (.env.dev):**
```env
APP_PORT=5333
```

**Production (.env.prod):**
```env
APP_PORT=80  # Standard HTTP port
```

### Load Balancer Setup

```bash
# Instance 1
APP_PORT=5333 docker-compose -p etl1 up -d

# Instance 2
APP_PORT=5334 docker-compose -p etl2 up -d

# Instance 3
APP_PORT=5335 docker-compose -p etl3 up -d

# Nginx load balancer
upstream etl_backend {
    server localhost:5333;
    server localhost:5334;
    server localhost:5335;
}
```

### Port Conflict Resolution

```bash
# Check nếu port đang được sử dụng
netstat -ano | findstr :5333

# Nếu conflict, đổi port
# Sửa .env
APP_PORT=5334

# Restart
docker-compose down
docker-compose up -d
```

## 🔧 Advanced Configuration

### 1. Dynamic EXPOSE trong Dockerfile

```dockerfile
# Build argument cho port
ARG PORT=5333
ENV APP_PORT=${PORT}
EXPOSE ${APP_PORT}
```

Build với custom port:
```bash
docker build --build-arg PORT=8080 -t etl-web:8080 .
```

### 2. Runtime port override

```bash
# Override tất cả env vars
docker run -e APP_PORT=9000 -p 9000:9000 etl-web:latest
```

### 3. Multiple compose files

**docker-compose.yml** (base):
```yaml
services:
  web:
    build: .
    image: etl-web:latest
```

**docker-compose.override.yml** (dev):
```yaml
services:
  web:
    environment:
      APP_PORT: 5333
    ports:
      - "5333:5333"
```

**docker-compose.prod.yml** (production):
```yaml
services:
  web:
    environment:
      APP_PORT: 80
    ports:
      - "80:80"
```

Usage:
```bash
# Dev
docker-compose up -d

# Production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## 🐛 Troubleshooting

### Issue: Health check fails

**Check:**
```bash
docker inspect etl-web --format='{{json .State.Health}}' | jq
```

**Common causes:**
1. Port mismatch (APP_PORT trong env khác với Flask port)
2. Flask chưa start xong (increase start-period)
3. requests library chưa install

**Solution:**
```dockerfile
# Tăng start-period
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import os, requests; requests.get(f'http://localhost:{os.getenv(\"APP_PORT\", \"5333\")}')"
```

### Issue: Port mapping không đúng

**Check docker-compose.yml:**
```yaml
# BAD
ports:
  - "5333:${APP_PORT}"  # Host fixed, container dynamic

# GOOD
ports:
  - "${APP_PORT:-5333}:${APP_PORT:-5333}"  # Both dynamic
```

### Issue: APP_PORT không được đọc

**Check .env file:**
```bash
# Verify .env exists
ls -la .env

# Check content
cat .env | grep APP_PORT
```

**Check environment trong container:**
```bash
docker-compose exec web env | grep APP_PORT
```

### Issue: Python không đọc được env var

**Check app.py:**
```python
# BAD
port = os.getenv('APP_PORT')  # Returns string or None
app.run(port=port)  # Error if None

# GOOD
port = int(os.getenv('APP_PORT', '5333'))  # Default value
app.run(port=port)
```

## 📊 Default Values Hierarchy

```
1. Environment variable (highest priority)
   APP_PORT=8080 docker-compose up -d

2. .env file
   APP_PORT=5333

3. docker-compose.yml default
   ${APP_PORT:-5333}

4. Dockerfile ENV
   ENV APP_PORT=5333

5. app.py hardcode (lowest priority)
   port = int(os.getenv('APP_PORT', '5333'))
```

## 🔒 Security Notes

### Production recommendations:

1. **Non-privileged ports** (> 1024):
   ```env
   APP_PORT=8080  # Good
   APP_PORT=80    # Requires root (bad)
   ```

2. **Firewall rules**:
   ```bash
   # Only allow specific ports
   ufw allow 5333/tcp
   ```

3. **Reverse proxy**:
   ```
   Internet → Nginx (80/443) → Flask (5333)
   ```

## 📈 Best Practices

### ✅ DO

- ✅ Use .env for configuration
- ✅ Provide default values (${APP_PORT:-5333})
- ✅ Document port in README
- ✅ Use health checks with correct port
- ✅ Test port changes before deploy

### ❌ DON'T

- ❌ Hardcode port in multiple places
- ❌ Forget to rebuild after port change
- ❌ Use privileged ports (< 1024) without good reason
- ❌ Expose unnecessary ports

## 📚 Related

- [PORT_CONFIG.md](PORT_CONFIG.md) - Original port config guide
- [.env](.env) - Environment configuration
- [docker-compose.yml](docker-compose.yml) - Service config
- [Dockerfile](Dockerfile) - Image config

---

**Port configuration is now fully dynamic!** 🎯

Change `APP_PORT` in `.env` and restart:
```bash
docker-compose down
docker-compose up -d --build
```
