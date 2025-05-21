# PROG8850Week1Installation
install mysql, python

```bash
ansible-playbook up.yml
```

To use mysql:

```bash
mysql -u root -h 127.0.0.1 -p
```

To shut down:

```bash
ansible-playbook down.yml
```

This is a reproducible mysql setup