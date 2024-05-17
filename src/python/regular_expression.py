import  re

text = "concats us at email@example.com or support@example.com"
pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'

email = re.findall(pattern,text)
print(email)