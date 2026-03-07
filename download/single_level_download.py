import cdsapi
import numpy as np
import os


def main():
    c = cdsapi.Client()

    download_dir = './download/reanalysis-era5-single-levels'

    variables = ['10m_u_component_of_wind', '10m_v_component_of_wind',
                 '2m_dewpoint_temperature', '2m_temperature',
                 'surface_pressure', 'surface_solar_radiation_downwards',
                 'surface_thermal_radiation_downwards', 'snowfall',
                 'total_precipitation', 'total_cloud_cover']
    
    download_years = np.arange(1996, 2026)

    download_info_all = []

    for year in reversed(download_years):
        for variable in variables:
            download_info_all.append({'variable': variable,
                                      'year': str(year)})

    for download_info in download_info_all:
        variable_name = download_info['variable']
        year = download_info['year']
        filename = f'{download_dir}/reanalysis-era5-single-levels_{variable_name}_{year}.nc'

        os.makedirs(f'download_dir/{year}', exist_ok=True)
        
        if os.path.exists(filename):
            print(f'{filename} already exists.')
            continue
        print(f'Downloading {variable_name} for {year}...')

        # Download the data
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': variable_name,
                'year': year,
                'month': [
                    '01', '02', '03',
                    '04', '05', '06',
                    '07', '08', '09',
                    '10', '11', '12',
                ],
                'day': [
                    '01', '02', '03',
                    '04', '05', '06',
                    '07', '08', '09',
                    '10', '11', '12',
                    '13', '14', '15',
                    '16', '17', '18',
                    '19', '20', '21',
                    '22', '23', '24',
                    '25', '26', '27',
                    '28', '29', '30',
                    '31',
                ],
                'time': [
                    '00:00', '01:00', '02:00',
                    '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00',
                    '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00',
                    '21:00', '22:00', '23:00',
                ],
            },
            f'{download_dir}/{year}/reanalysis-era5-single-levels_{variable_name}_{year}.nc'
        )


if __name__ == "__main__":
    main()
