from datetime import date

EXPIRY_DATES = {
    '21':{
        'DEC': {
            'WEEK1':['21NOV24','21DEC02','21DEC30'],
            'WEEK2':['21DEC01','21DEC09','21DEC30'],
            'WEEK3':['21DEC08','21DEC16','21DEC30'],
            'WEEK4':['21DEC15','21DEC23','21DEC30'],
            'WEEK5':['21DEC22','21DEC30','21DEC30']
        },
        'NOV': {
            #'WEEK1':['21OCT27','21NOV03','21NOV25'],
            'WEEK2':['21NOV03','21NOV11','21NOV25'],
            'WEEK3':['21NOV10','21NOV18','21NOV25'],
            'WEEK4':['21NOV17','21NOV25','21NOV25']
        },
        'OCT': {
            'WEEK1':['21SEP29','21OCT07','21OCT28'],
            'WEEK2':['21OCT06','21OCT14','21OCT28'],
            'WEEK3':['21OCT13','21OCT21','21OCT28'],
            'WEEK4':['21OCT20','21OCT28','21OCT28']
        },
        'SEP': {
            'WEEK1':['21AUG25','21SEP02','21SEP30'],
            'WEEK2':['21SEP01','21SEP09','21SEP30'],
            'WEEK3':['21SEP08','21SEP16','21SEP30'],
            'WEEK4':['21SEP15','21SEP23','21SEP30'],
            'WEEK5':['21SEP22','21SEP30','21SEP30']
        },
        'AUG': {
            'WEEK1':['21JUL28','21AUG05','21AUG26'],
            'WEEK2':['21AUG04','21AUG12','21AUG26'],
            'WEEK3':['21AUG11','21AUG18','21AUG26'],
            'WEEK4':['21AUG18','21AUG26','21AUG26']
        },
        'JUL': {
            'WEEK1':['21JUN23','21JUL01','21JUL29'],
            'WEEK2':['21JUN30','21JUL08','21JUL29'],
            'WEEK3':['21JUL07','21JUL15','21JUL29'],
            'WEEK4':['21JUL14','21JUL22','21JUL29'],
            'WEEK5':['21JUL21','21JUL29','21JUL29']
        },
        'JUN': {
            'WEEK1':['21MAY26','21JUN03','21JUN24'],
            'WEEK2':['21JUN02','21JUN10','21JUN24'],
            'WEEK3':['21JUN09','21JUN17','21JUN24'],
            'WEEK4':['21JUN16','21JUN24','21JUN24']
        },
        'MAY': {
            'WEEK1':['21APR28','21MAY06','21MAY27'],
            'WEEK2':['21MAY05','21MAY12','21MAY27'],
            'WEEK3':['21MAY12','21MAY20','21MAY27'],
            'WEEK4':['21MAY19','21MAY27','21MAY27']
         },
        'APR': {
            'WEEK1':['21MAR24','21APR01','21APR29'],
            'WEEK2':['21MAR31','21APR08','21APR29'],
            'WEEK3':['21APR07','21APR15','21APR29'],
        #     'WEEK4':['21APR14','21APR22','21APR29'],
        #     'WEEK5':['21APR21','21APR29','21APR29']
        },
        # 'MAR': {
        #     'WEEK1':['21FEB24','21MAR04','21MAR25'],
        #     'WEEK2':['21MAR03','21MAR10','21MAR25'],
        #     'WEEK3':['21MAR10','21MAR18','21MAR25'],
        #     'WEEK4':['21MAR17','21MAR25','21MAR25']
        # },
        # 'FEB': {
        #     'WEEK1':['21JAN27','21FEB04','21FEB25'],
        #     'WEEK2':['21FEB03','21FEB11','21FEB25'],
        #     'WEEK3':['21FEB10','21FEB18','21FEB25'],
        #     'WEEK4':['21FEB17','21FEB25','21FEB25']
        # },
        # 'JAN': {
        #     'WEEK1':['20DEC30','21JAN07','21JAN28'],
        #     'WEEK2':['21JAN06','21JAN14','21JAN28'],
        #     'WEEK3':['21JAN13','21JAN21','21JAN28'],
        #     'WEEK4':['21JAN20','21JAN28','21JAN28']
        # }
    },
    '22':{
        # 'MAR': {
        #     'WEEK1':['22FEB23','22MAR03','22MAR31'],
        #     #'WEEK2':['22MAR02','22MAR10','22MAR31'],
        #     'WEEK3':['22MAR09','22MAR17','22MAR31'],
        #     'WEEK4':['22MAR16','22MAR24','22MAR31'],
        #     'WEEK5':['22MAR23','22MAR31','22MAR31']
        # },
        'FEB': {
            # 'WEEK1':['22JAN26','22FEB03','22FEB24'],
            # 'WEEK2':['22FEB02','22FEB10','22FEB24'],
            'WEEK3':['22FEB09','22FEB17','22FEB24'],
            'WEEK4':['22FEB16','22FEB24','22FEB24']
        },
        'JAN': {
            'WEEK1':['21DEC29','22JAN06','22JAN27'],
            'WEEK2':['22JAN05','22JAN13','22JAN27'],
            'WEEK3':['22JAN12','22JAN20','22JAN27'],
            'WEEK4':['22JAN19','22JAN27','22JAN27']
        }
    }
}

EXPIRY_DATES_WED = {
    '21':{
        'DEC': {
            'WEEK1':['21NOV24','21DEC02','21DEC30'],
            'WEEK2':['21DEC01','21DEC09','21DEC30'],
            'WEEK3':['21DEC08','21DEC16','21DEC30'],
            'WEEK4':['21DEC15','21DEC23','21DEC30'],
            'WEEK5':['21DEC22','21DEC30','21DEC30']
        },
        'NOV': {
            'WEEK1':['21OCT27','21NOV03','21NOV25'],
            'WEEK2':['21NOV03','21NOV11','21NOV25'],
            'WEEK3':['21NOV10','21NOV18','21NOV25'],
            'WEEK4':['21NOV17','21NOV25','21NOV25']
        },
        'OCT': {
            'WEEK1':['21SEP29','21OCT07','21OCT28'],
            'WEEK2':['21OCT06','21OCT14','21OCT28'],
            'WEEK3':['21OCT13','21OCT21','21OCT28'],
            'WEEK4':['21OCT20','21OCT28','21OCT28']
        },
        'SEP': {
            'WEEK1':['21AUG25','21SEP02','21SEP30'],
            'WEEK2':['21SEP01','21SEP09','21SEP30'],
            'WEEK3':['21SEP08','21SEP16','21SEP30'],
            'WEEK4':['21SEP15','21SEP23','21SEP30'],
            'WEEK5':['21SEP22','21SEP30','21SEP30']
        },
        'AUG': {
            'WEEK1':['21JUL28','21AUG05','21AUG26'],
            'WEEK2':['21AUG04','21AUG12','21AUG26'],
            'WEEK3':['21AUG11','21AUG18','21AUG26'],
            'WEEK4':['21AUG18','21AUG26','21AUG26']
        },
        'JUL': {
            'WEEK1':['21JUN23','21JUL01','21JUL29'],
            'WEEK2':['21JUN30','21JUL08','21JUL29'],
            'WEEK3':['21JUL07','21JUL15','21JUL29'],
            'WEEK4':['21JUL14','21JUL22','21JUL29'],
            'WEEK5':['21JUL21','21JUL29','21JUL29']
        },
        'JUN': {
            'WEEK1':['21MAY26','21JUN03','21JUN24'],
            'WEEK2':['21JUN02','21JUN10','21JUN24'],
            'WEEK3':['21JUN09','21JUN17','21JUN24'],
            'WEEK4':['21JUN16','21JUN24','21JUN24']
        },
        'MAY': {
            'WEEK1':['21APR28','21MAY06','21MAY27'],
            'WEEK2':['21MAY05','21MAY12','21MAY27'],
            'WEEK3':['21MAY12','21MAY20','21MAY27'],
            'WEEK4':['21MAY19','21MAY27','21MAY27']
         },
        'APR': {
            'WEEK1':['21MAR24','21APR01','21APR29'],
            'WEEK2':['21MAR31','21APR08','21APR29'],
            'WEEK3':['21APR07','21APR15','21APR29'],
            'WEEK4':['21APR14','21APR22','21APR29'],
            'WEEK5':['21APR21','21APR29','21APR29']
         },
        'MAR': {
            'WEEK1':['21FEB24','21MAR04','21MAR25'],
            'WEEK2':['21MAR03','21MAR10','21MAR25'],
            'WEEK3':['21MAR10','21MAR18','21MAR25'],
            'WEEK4':['21MAR17','21MAR25','21MAR25']
        },
        'FEB': {
            'WEEK1':['21JAN27','21FEB04','21FEB25'],
            'WEEK2':['21FEB03','21FEB11','21FEB25'],
            'WEEK3':['21FEB10','21FEB18','21FEB25'],
            'WEEK4':['21FEB17','21FEB25','21FEB25']
        },
        'JAN': {
            'WEEK1':['20DEC30','21JAN07','21JAN28'],
            'WEEK2':['21JAN06','21JAN14','21JAN28'],
            'WEEK3':['21JAN13','21JAN21','21JAN28'],
            'WEEK4':['21JAN20','21JAN28','21JAN28']
        }
    },
    '22':{
        'JAN': {
            'WEEK1':['21DEC29','22JAN06','22JAN27'],
            'WEEK2':['22JAN05','22JAN13','22JAN27'],
            'WEEK3':['22JAN12','22JAN20','22JAN27'],
            'WEEK4':['22JAN19','22JAN27','22JAN27']
        }
    }
}

EXPIRY_DATES_THU = {
    '21':{
        'DEC': {
            'WEEK1':['21NOV25','21DEC02','21DEC30'],
            'WEEK2':['21DEC02','21DEC09','21DEC30'],
            'WEEK3':['21DEC09','21DEC16','21DEC30'],
            'WEEK4':['21DEC16','21DEC23','21DEC30'],
            'WEEK5':['21DEC23','21DEC30','21DEC30']
        },
        'NOV': {
            'WEEK1':['21OCT28','21NOV03','21NOV25'],
            'WEEK2':['21NOV03','21NOV11','21NOV25'],
            'WEEK3':['21NOV11','21NOV18','21NOV25'],
            'WEEK4':['21NOV18','21NOV25','21NOV25']
        },
        'OCT': {
            'WEEK1':['21SEP30','21OCT07','21OCT28'],
            'WEEK2':['21OCT07','21OCT14','21OCT28'],
            'WEEK3':['21OCT14','21OCT21','21OCT28'],
            'WEEK4':['21OCT21','21OCT28','21OCT28']
        },
        'SEP': {
            'WEEK1':['21AUG26','21SEP02','21SEP30'],
            'WEEK2':['21SEP02','21SEP09','21SEP30'],
            'WEEK3':['21SEP09','21SEP16','21SEP30'],
            'WEEK4':['21SEP16','21SEP23','21SEP30'],
            'WEEK5':['21SEP23','21SEP30','21SEP30']
        },
        'AUG': {
            'WEEK1':['21JUL29','21AUG05','21AUG26'],
            'WEEK2':['21AUG05','21AUG12','21AUG26'],
            'WEEK3':['21AUG12','21AUG18','21AUG26'],
            'WEEK4':['21AUG18','21AUG26','21AUG26']
        },
        'JUL': {
            'WEEK1':['21JUN24','21JUL01','21JUL29'],
            'WEEK2':['21JUL01','21JUL08','21JUL29'],
            'WEEK3':['21JUL08','21JUL15','21JUL29'],
            'WEEK4':['21JUL15','21JUL22','21JUL29'],
            'WEEK5':['21JUL22','21JUL29','21JUL29']
        },
        'JUN': {
            'WEEK1':['21MAY27','21JUN03','21JUN24'],
            'WEEK2':['21JUN03','21JUN10','21JUN24'],
            'WEEK3':['21JUN10','21JUN17','21JUN24'],
            'WEEK4':['21JUN17','21JUN24','21JUN24']
        },
        'MAY': {
            'WEEK1':['21APR28','21MAY06','21MAY27'],
            'WEEK2':['21MAY06','21MAY12','21MAY27'],
            'WEEK3':['21MAY12','21MAY20','21MAY27'],
            'WEEK4':['21MAY20','21MAY27','21MAY27']
         },
        'APR': {
            'WEEK1':['21MAR25','21APR01','21APR29'],
            'WEEK2':['21APR01','21APR08','21APR29'],
            'WEEK3':['21APR08','21APR15','21APR29'],
            'WEEK4':['21APR15','21APR22','21APR29'],
            'WEEK5':['21APR22','21APR29','21APR29']
         }
    }
}


HOLIDAY_LIST = {
    '2021' : [date(2021, 1, 26),  #Mahashivratri
                date(2021, 3, 11),  #Mahavir Jayanti
                date(2021, 3, 29),  #Good Friday
                date(2021, 4, 2),  #Maharashtra Day
                date(2021, 4, 14),  #Id Ul Fitra
                date(2021, 4, 21),  #Bakri Id
                date(2021, 5, 13),  # Good Friday
                date(2021, 7, 21),  # Maharashtra Day
                date(2021, 8, 19),  # Id Ul Fitra
                date(2021, 9, 10),
                date(2021, 10, 15),  # Id Ul Fitra
                date(2021, 11, 4),
                date(2021, 11, 5),
                date(2021, 11, 19)
                ],
    '2022' : [date(2022, 1, 26),  #Republic Day
                date(2022, 3, 1),   #Mahashivratri
                date(2022, 3, 18),  #Holi
                date(2022, 4, 14),  #Ambedkar Jayanti
                date(2022, 4, 15),  #Good Friday
                date(2022, 5, 3),   #Bakri Id
                date(2022, 8, 9),   #Moharram
                date(2022, 8, 15),  #Independence Day
                date(2022, 8, 31),  #Ganesh Chaturthi
                date(2022, 10, 5),  #Dussehra
                date(2022, 10, 24), #Diwali-Laxmi Pujan
                date(2022, 10, 26), #Diwali-Balipratipada
                date(2022, 11, 8)   #Gurunanak Jayanti
                ]
}