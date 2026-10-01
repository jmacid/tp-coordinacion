# Resolucion

## 2 - Multiples clientes
Para soportar multiples clientes se realizo una multiplexacion logica de los clientes. Para ello, se incluyo en los mensajes el id de cada cliente para poder diferenciar los flujos entre ellos. La modificacion principal se produjo en el **gateway** donde el `id` es generado de forma trasparente para los clientes. El **Gateway** guarda un mapa o lista en memoria relacionando ese `client_id` con su respectivo socket de conexión. Cuando el resultado final (`[client_id, fruit_top]`) vuelve desde el nodo **Joiner**, el **Gateway** busca el socket correcto en su lista y le envía la respuesta exclusivamente a ese cliente, garantizando que nadie reciba el top de otro.
Tambien se produjeron cambios en **Sum** y en el **Aggregator**, donde se empezo a agrupar los datos por `client_id`. Es decir, los datos de cada cliente van a su propia lista. Además, al finalizar el procesamiento de un cliente (al recibir su señal `EOF`), cada nodo elimina únicamente la estructura de datos asociada a ese `client_id`, liberando memoria sin afectar los flujos de los demás clientes activos.

## 3 - Multiples sum

Se utiliza un exchange (`SUM_CONTROL_EXCHANGE`) para que los SUMs se avisen entre sí de los EOFs. Se utiliza un *thread* para que escuche por los mensajes EOF y se utiliza `add_callback_threadsafe` para que no haya condicion de carrera con los mensajes de frutras y se cierre prematuramente. Esto inyecta la ejecución de la función directamente en el event loop del hilo principal, asegurando que se ejecute en orden junto con los mensajes que ya están encolados localmente.

Para los aggregators, para esperar a todos los sums para que envien el EOF se utiliza un contador y se espera hasta recibir `SUM_AMOUNT` para proceder. Ademas se utiliza un exchange para publicar y otro para consumir.

## 4 - Multiples aggregators
Para contemplar multiples aggregators los sum van a mandar cada tipo de fruta a un aggregator en particular. Para ello se hashea el nombre de la fruta y en base a esa transformacion se envia el mensaje. Por lo tano, la fruta 'manzana' siempre se enviara al mismo aggregator.

Por ultimo, el join se modifica para contemplar los "EOFs" de los aggregators y esperar agrupar las cantidades por cliente hasta recibir el ultimo "EOF".