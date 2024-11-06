## Comando para subir ambiente no console (precisa usar java 17)
`sbt console -java-home C:\Users\F6780837\.vscode\extensions\redhat.java-1.36.0-win32-x64\jre\17.0.13-win32-x86_64`

## Configurar hadoop (nao funcionou com winutils mas o proprio repo do hadoop sugere usar o bare-naked-local-fs)
[https://stackoverflow.com/questions/73503205/why-all-these-hadoop-home-and-winutils-errors-with-spark-on-windows-if-hadoop]
 [https://github.com/steveloughran/winutils?tab=readme-ov-file]
  [https://github.com/globalmentor/hadoop-bare-naked-local-fs] - não funcionou no scala, então fui no cdarlint que o repo do carinha do hadoop sugeriu no próprio README
  [https://github.com/cdarlint/winutils/tree/master]

## Carinha do medium sobre pyspark, ajudou pra caramba:
https://medium.com/@dipan.saha/getting-started-with-pyspark-day-1-37e5e6fdc14b