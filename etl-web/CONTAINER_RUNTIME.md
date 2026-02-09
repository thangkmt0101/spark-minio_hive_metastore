# Container Runtime Configuration

Hướng dẫn về cấu hình để container chạy mãi mãi và không tự tắt.

## 🔧 Đã cấu hình

### 1. Dockerfile

```dockerfile
# Run Flask app (foreground mode - chạy mãi không tắt)
CMD ["python", "-u", "app.py"]
```

**Giải thích**:
- `python -u`: Unbuffered mode - output ngay lập tức (không buffer)
- `app.py`: Main process chạy ở foreground

### 2. app.py

```python
if __name__ == '__main__':
    port = int(os.getenv('APP_PORT', '5333'))
    
    app.run(
        debug=os.getenv('FLASK_DEBUG', '0') == '1',
        host='0.0.0.0',
        port=port,
        use_reloader=False,  # Không auto-reload
        threaded=True        # Enable threading
    )
```

**Key settings**:
- ✅ `use_reloader=False`: Không restart tự động (stable container)
- ✅ `host='0.0.0.0'`: Listen trên tất cả interfaces
- ✅ `threaded=True`: Handle multiple requests đồng thời

### 3. docker-compose.yml

```yaml
web:
  restart: unless-stopped    # Auto restart nếu crash
  stdin_open: true          # Keep stdin open
  tty: true                 # Allocate TTY
  environment:
    FLASK_DEBUG: 0          # Production mode
```

**Restart policies**:
- `unless-stopped`: Container luôn chạy trừ khi user stop manually
- `always`: Luôn restart (ngay cả khi reboot server)
- `on-failure`: Chỉ restart khi exit code != 0
- `no`: Không auto restart

## ✅ Kết quả

Container sẽ:
- ✅ **Chạy mãi mãi** - Process Flask không exit
- ✅ **Auto restart** - Nếu crash thì tự động restart
- ✅ **Stable** - Không auto-reload gây restart
- ✅ **Logging** - Output unbuffered, xem logs real-time

## 🚀 Test

### Start container

```bash
docker-compose up -d
```

### Verify container đang chạy

```bash
# Check status
docker-compose ps

# Output:
# NAME        STATUS              PORTS
# etl-web     Up 5 minutes        0.0.0.0:5333->5333/tcp
```

### Xem logs real-time

```bash
docker-compose logs -f web
```

### Check uptime

```bash
# Xem container chạy bao lâu
docker ps --format "table {{.Names}}\t{{.Status}}"

# Output:
# NAMES       STATUS
# etl-web     Up 2 hours
# etl-postgres Up 2 hours
```

## 🔍 Troubleshooting

### Container tự tắt sau vài giây

**Nguyên nhân**: Process chính exit quá nhanh

**Giải pháp**:
```bash
# Xem logs để tìm error
docker-compose logs web

# Check exit code
docker inspect etl-web --format='{{.State.ExitCode}}'
```

### Container restart liên tục

**Nguyên nhân**: Có lỗi trong app.py hoặc không kết nối được DB

**Giải pháp**:
```bash
# Xem logs
docker-compose logs web

# Check health
docker inspect etl-web --format='{{.State.Health.Status}}'
```

### Muốn container stop khi có error

Sửa `restart: no` trong docker-compose.yml:

```yaml
web:
  restart: no  # Không auto restart
```

## 📊 Monitoring

### Xem resource usage

```bash
# Real-time stats
docker stats etl-web

# Output:
# CONTAINER   CPU %   MEM USAGE / LIMIT   NET I/O
# etl-web     0.5%    128MB / 512MB       1kB / 2kB
```

### Health check status

```bash
# Check health
docker inspect etl-web --format='{{.State.Health.Status}}'

# View health check logs
docker inspect etl-web --format='{{json .State.Health}}' | jq
```

## 🎯 Best Practices

### ✅ Production Settings

```yaml
web:
  restart: unless-stopped
  environment:
    FLASK_DEBUG: 0
    PYTHONUNBUFFERED: 1
  healthcheck:
    interval: 30s
    timeout: 10s
    retries: 3
```

### ✅ Development Settings

```yaml
web:
  restart: no  # Manual control
  environment:
    FLASK_DEBUG: 1
  volumes:
    - .:/app  # Hot reload với mounted source
```

## 🔄 Lifecycle Management

### Graceful Shutdown

```bash
# Stop container gracefully (SIGTERM)
docker-compose stop web

# Stop immediately (SIGKILL)
docker-compose kill web

# Remove container
docker-compose rm web
```

### Restart Container

```bash
# Restart single service
docker-compose restart web

# Restart all services
docker-compose restart
```

### Update and Restart

```bash
# Rebuild và restart
docker-compose up -d --build

# Zero-downtime với scale
docker-compose up -d --scale web=2
docker-compose up -d --scale web=1
```

## 💡 Advanced: Keep-Alive Patterns

### Option 1: Flask Built-in (Current)

```python
app.run(host='0.0.0.0', port=5333, use_reloader=False)
```

✅ Simple, works well for most cases

### Option 2: Production WSGI Server

```dockerfile
# Install gunicorn
RUN pip install gunicorn

# Run with gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:5333", "-w", "4", "app:app"]
```

✅ Better performance, production-ready

### Option 3: Supervisor

```dockerfile
# Install supervisor
RUN apt-get install -y supervisor

# Run with supervisor (process manager)
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
```

✅ Advanced process management

## 📝 Current Configuration Summary

| Setting | Value | Purpose |
|---------|-------|---------|
| CMD | `python -u app.py` | Run unbuffered |
| use_reloader | `False` | No auto-restart |
| restart | `unless-stopped` | Auto restart on crash |
| stdin_open | `true` | Keep stdin open |
| tty | `true` | Allocate TTY |
| FLASK_DEBUG | `0` | Production mode |

## ✅ Verification Checklist

Sau khi start container, check:

- [ ] Container status: `docker-compose ps` → "Up"
- [ ] Uptime: Chạy > 5 phút không restart
- [ ] Logs: Không có error critical
- [ ] Health: `docker inspect etl-web` → "healthy"
- [ ] Access: http://localhost:5333 → Working

---

**Kết luận**: Container giờ đây sẽ chạy mãi mãi và chỉ stop khi:
1. User chạy `docker-compose down`
2. User chạy `docker-compose stop`
3. Server reboot (và sẽ auto restart khi server up lại)

**Không tự tắt** nữa! 🎉
