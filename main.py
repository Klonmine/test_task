import mysql.connector , datetime,math
from mysql.connector import Error
from secret_info import host,database,user,password

try:
    finish_list = []    # Здесь будет храниться результат
    # Если взять просто текущее время,то скрипт не отработает корректно.Так что возьму максимальную дату из бд
    now=datetime.datetime.now()
    #now = datetime.date(now.year, now.month, now.day) всегда можно запустить данный скрипт используя текущую дату и время
    now=datetime.date(2019,3,20)

    # Подключаемся к базе данных
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=user,
        password=password # Здесь пароль введите сами
    )

    if connection.is_connected():
        print("Успешно подключились к базе данных")
        def main():
            #В этом блоке мы получаем интересующую нас оборудку из базы данных

            # Создание курсора для выполнения первого запроса
            cursor = connection.cursor()

            # С помощью этого запроса мы получили айдишник ящика и список айдишнико подключенной в него оборудки
            # Но не любой,а той которая интересует нас по условиям
            # Как я понял из задания signal_center - это актуальная оборудка ,которая используется
            # acc - Это DES , gb - DGS
            cursor.execute(
"""SELECT location_id, GROUP_CONCAT(id)  AS ids FROM device WHERE  
((location_type = 'signal_center') AND            
(port_count >= 24 OR port_count = 10)) AND   
(switch_role = 'acc' OR switch_role
= 'gb') GROUP BY location_id ORDER BY location_id ASC ;
"""
            )

            all_oborudka = cursor.fetchall()
            cursor.close()
            """
Получилось что-то вроде
+-------------+-----------------+
| location_id | ids             |
+-------------+-----------------+
|           7 | 9,227           |
|           8 | 10,286,9421     |
|          10 | 12,464,5933     |
|          11 | 13,158          |
|          13 | 15,38,240,18205 |
+-------------+-----------------+
5 rows in set (0,00 sec)
            """

            #В all_oborudka Получился такой список с кортежами
            #[(7, '9,227'), (8, '10,286,9421'), (10, '12,464,5933'), (11, '13,158'), (13, '15,38,240,18205'),
            for one_oborudka in all_oborudka:   # Обходим  список где лежит location id и id оборудки в этом ящике
                all_reg_date=[]
                all_ext_close_date=[]
                amount_free_100_ports=0
                amount_free_1000_ports=0
                amount_busy_100_ports=0
                amount_busy_1000_ports = 0
                amount_death_ports=0
                location_id=one_oborudka[0]
                # Здесь получили айдишники всех свичей в данном ящике
                ids = list(map(int, str(one_oborudka[1]).split(",")))
                len_ids=len(ids)
                schetchik=0 # Этот счётчик нужен для того чтобы понять что мы обошли все девайсы в ящике
                # ids = [9, 227] и подобное
                more_24_ports=False
                only_10_ports=False
                for id in ids:

                    schetchik+=1
                    cursor = connection.cursor()
                    # Сделаю запрос через F - строку ,ведь нет вероятности SQL - Инъекции
                    cursor.execute(
                        f"""SELECT switch_role FROM device WHERE id = {id};
                            """
                    )


                    switch_type = cursor.fetchall()[0][0]
                    cursor.close()


                    cursor = connection.cursor()
                    # Этим запросом узнаём на сколько портов свич
                    cursor.execute(
                        f"""SELECT MAX(port_id) FROM switch_port WHERE device_id = {id};
        """
                    )

                    max_amount_ports = cursor.fetchall()[0][0]
                    cursor.close()
                    # Обрабатываем условие где говорится что нужно учитывать определенное количество портов
                    if max_amount_ports>=24:
                        more_24_ports=True
                    elif max_amount_ports==10:
                        only_10_ports=True
                    if more_24_ports==True:
                        cursor = connection.cursor()
                        cursor.execute(
f"""SELECT * FROM switch_port WHERE 
(device_id = {id} AND port_id BETWEEN 1 AND 24 ) AND 
(binding_type = 'user' OR binding_type is NULL) ;
                                """
                        )
                        # Здесь мы получили исключительно абонентские порты на свиче
                        all_switch_port = cursor.fetchall()
                        cursor.close()

                    elif only_10_ports==True:
                        cursor = connection.cursor()
                        cursor.execute(
f"""SELECT * FROM switch_port WHERE 
(device_id = {id} AND port_id BETWEEN 1 AND 8 ) AND 
(binding_type = 'user' OR binding_type is NULL) ;
                                                        """
                        )
                        # Здесь мы получили исключительно абонентские порты на свиче
                        all_switch_port = cursor.fetchall()
                        cursor.close()

                    # Собираем информацию с каждого порта на свиче
                    for one_switch_port in all_switch_port:
                        binding_type=one_switch_port[2]
                        # Если binding_type - None -  значит порт свободен
                        if binding_type==None and switch_type=='acc':
                            amount_free_100_ports+=1
                        elif binding_type==None and switch_type=='gb':
                            amount_free_1000_ports+=1
                            # Если binding_type - user - значи нужно проверить активный ли это абонент или dead
                        elif binding_type=='user' and switch_type=='acc':
                            ext_close_date=""
                            # Получаем данные об этом абонента
                            cursor = connection.cursor()
                            cursor.execute(
                                f"""SELECT * FROM user_properties WHERE uid = {one_switch_port[3]};
                                """
                            )
                            # Здесь мы получили абонентские порты на свиче
                            """
                            all_of_this_user получится что-то вроде
                            [(35178,datetime.date(2014,1,16),1,'Активен',None)]
                            """
                            all_of_this_user = cursor.fetchall()
                            cursor.close()
                            if all_of_this_user != []:
                                if all_of_this_user[0][1]!=None:
                                    # Получаем значение когда этот абонент подключился и добавляем значение в список
                                    reg_date=all_of_this_user[0][1]
                                    all_reg_date.append(reg_date)
                                # Если статус договора 1 , то это определенно активный абонент
                                if all_of_this_user[0][2]==1:
                                    amount_busy_100_ports+=1
                                # Если статус договора 0 , то нужно проверить прошло ли 90 дней
                                else:
                                    ext_close_date = all_of_this_user[0][4]
                                    if  ext_close_date!=None:
                                        # Если абонент не оплачивает уже 90 дней,то считаем его мёртвым
                                        if now>ext_close_date+datetime.timedelta(days=90):
                                            all_ext_close_date.append(ext_close_date)
                                            amount_death_ports+=1
                                        else:
                                            # Если меньше 90 дней,то считаем его активным
                                            amount_busy_100_ports+=1



                        elif binding_type=='user' and switch_type=='gb':
                            ext_close_date = ""
                            # Получаем данные об этом абонента
                            cursor = connection.cursor()
                            cursor.execute(
                                f"""SELECT * FROM user_properties WHERE uid = {one_switch_port[3]};
                                                            """
                            )
                            # Здесь мы получили абонентские порты на свиче
                            all_of_this_user = cursor.fetchall()
                            cursor.close()
                            if all_of_this_user!=[]:
                                if all_of_this_user[0][1] != None:
                                    reg_date = all_of_this_user[0][1]
                                # Если статус договора 1 , то это определенно
                                if all_of_this_user[0][2] == 1:
                                    amount_busy_1000_ports += 1
                                else:
                                    ext_close_date = all_of_this_user[0][4]
                                    if ext_close_date != None:
                                        if now > ext_close_date + datetime.timedelta(days=90):
                                            amount_death_ports += 1
                                        else:
                                            amount_busy_1000_ports += 1


                    #Это условие при котором прога обработала все свичи в этом ящике
                    if schetchik==len_ids:

                        new_all_ext_close_date=[]
                        new_all_reg_date=[]
                        # Находим даты отключения за полгода в этом ящике
                        for one_ext_close_date in all_ext_close_date:
                            if one_ext_close_date>now-datetime.timedelta(weeks=4*6):
                                new_all_ext_close_date.append(one_ext_close_date)

                        schetchik_recently_reg=0
                        # Находим даты подключения за полгода в этом ящике
                        for one_reg_date in all_reg_date:
                            if one_reg_date>now-datetime.timedelta(weeks=4*6):
                                new_all_reg_date.append(one_reg_date)
                                # Проверяем как много новых абонентов в ящике
                                if one_reg_date>now-datetime.timedelta(weeks=4):
                                    schetchik_recently_reg+=1

                        # Количество подкл-откл абонентов
                        difference_users_for_half_year = (len(new_all_reg_date) - len(new_all_ext_close_date))
                        difference_users_for_one_month=math.ceil(difference_users_for_half_year/6)
                        difference_users_for_one_month=int(difference_users_for_one_month)

                        forecast_no_free_ports=""
                        amount_free_ports = amount_free_100_ports + amount_free_1000_ports + amount_death_ports
                        if difference_users_for_one_month>0:
                            forecast_no_free_ports=amount_free_ports/difference_users_for_one_month
                            # Считаем что если больше 10 абонентов подключились за месяц,то это новый дом
                            if schetchik_recently_reg>10:
                                forecast_no_free_ports=forecast_no_free_ports/3
                        elif difference_users_for_one_month<0:
                            forecast_no_free_ports = amount_free_ports / abs(difference_users_for_one_month)
                        else:
                            """
                            Либо Одинаковое количество откл/подкл 
                            Либо на этом оборудовании очень давно никто не подключался и не отключался
                            """
                            # В этом случае ,допустим, что порты не закончатся раньше чем через год
                            forecast_no_free_ports=12


                        finish_list.append({
                            'location_id': location_id,
                            'used_100': amount_busy_100_ports,
                            'user_gb': amount_busy_1000_ports,
                            'free_100': amount_free_100_ports,
                            'free_gb': amount_free_1000_ports,
                            'dead_ports': amount_death_ports,
                            'forecast_no_free_ports': forecast_no_free_ports,

                        })

                        pass
            print(finish_list)


except Error as e:
    print(f"Ошибка подключения к базе данных: {e}")
if __name__=="__main__":
    main()

