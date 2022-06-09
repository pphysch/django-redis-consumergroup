# Use this Vagrantfile to spin up a development environment!
# 1. Install Vagrant and a provider (e.g. VirtualBox). You will also need Ansible installed.
# 2. run `vagrant up` in this directory.

Vagrant.configure("2") do |config|
    config.vm.box = "rockylinux/8"
    config.vm.box_version = "< 5.0.0"

    config.vm.define "drcg-dev" do |django|
        django.vm.synced_folder ".", "/opt/django"#, mount_options: ["ro"]
        django.vm.network "forwarded_port", guest: 8080, host: 8080
        django.vm.network "forwarded_port", guest: 8888, host: 8888
        #django.vm.network "forwarded_port", guest: 443, host: 8443

        #django.vm.provision "ansible" do |ansible|
        #    ansible.playbook = "provisioning/playbook.yml"
        #    ansible.extra_vars = {
        #        website_address: django.vm.hostname
        #    }
        #    #ansible.verbose = 'v'
        #end
    end
end