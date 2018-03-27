from setuptools import setup  
setup(name='harmonicIO',
	version='0.2.0',
	packages=[
		'harmonicIO',
		'harmonicIO.stream_connector'],
	entry_points={
		'console_scripts' : [
			'simulator = simulator.__main__:main'
		]
	}
)
