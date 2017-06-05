#xyr build defines
# This section will be re-written by Jenkins build system.
%define _xyr_package_name     sspl_hl
%define _xyr_package_source   sspl-1.0.0.tgz
%define _xyr_package_version  1.0.0
%define _xyr_build_number     1.el7
%define _xyr_pkg_url          http://es-gerrit:8080/sspl
%define _xyr_svn_version      0
#xyr end defines

%define installpath %{buildroot}/opt/plex/apps

Name:       %{_xyr_package_name}
Version:    %{_xyr_package_version}
Release:    %{_xyr_build_number}
Summary:    Seagate System Platform Library - High Level

Group:      Applications/System
License:    Seagate Proprietary
URL:        %{_xyr_pkg_url}
Source0:    %{_xyr_package_source}
Vendor:     Seagate Technology LLC
BuildArch:  noarch

BuildRequires: python >= 2.7.0
Requires:   PLEX, python-paramiko, python-dateutil, botocore, boto3, jmespath, xmltodict

%description
Seagate System Platform Library - High Level

A cli (and library) that allow the user to control the cluster.


%prep
%setup -q -n sspl/high-level


%build


%install
mkdir -p %{installpath}/sspl_hl/views
mkdir -p %{installpath}/sspl_hl/providers/{response,power,bundle,status}
mkdir -p %{installpath}/sspl_hl/utils
mkdir -p %{installpath}/sspl_hl/utils/cluster_node_manager
mkdir -p %{installpath}/sspl_hl/utils/support_bundle
mkdir -p %{installpath}/sspl_hl/utils/support_bundle/file_collector
install sspl_hl/utils/*.py %{installpath}/sspl_hl/utils/
install sspl_hl/utils/cluster_node_manager/*.py %{installpath}/sspl_hl/utils/cluster_node_manager/
install sspl_hl/utils/support_bundle/*.py %{installpath}/sspl_hl/utils/support_bundle/
install sspl_hl/utils/support_bundle/file_collector/*.py %{installpath}/sspl_hl/utils/support_bundle/file_collector/
install sspl_hl/main.py sspl_hl/__init__.py %{installpath}/sspl_hl/
install sspl_hl/providers/__init__.py %{installpath}/sspl_hl/providers/
install sspl_hl/providers/power/*.py %{installpath}/sspl_hl/providers/power/
install sspl_hl/providers/response/*.py %{installpath}/sspl_hl/providers/response/
install sspl_hl/providers/bundle/*.py %{installpath}/sspl_hl/providers/bundle/
install sspl_hl/providers/status/*.py %{installpath}/sspl_hl/providers/status/


mkdir -p %{buildroot}/usr/lib/python2.7/site-packages/cstor/cli/commands
install cstor/__init__.py %{buildroot}/usr/lib/python2.7/site-packages/cstor/
install cstor/cli/*.py %{buildroot}/usr/lib/python2.7/site-packages/cstor/cli/
install cstor/cli/main.py %{buildroot}/usr/lib/python2.7/site-packages/cstor/cli/
install cstor/cli/commands/*.py %{buildroot}/usr/lib/python2.7/site-packages/cstor/cli/commands/

mkdir -p %{buildroot}/usr/bin
ln -s /usr/lib/python2.7/site-packages/cstor/cli/main.py %{buildroot}/usr/bin/cstor

mkdir -p %{buildroot}/var/lib/support_bundles

%files
%defattr(0644,plex,plex,-)
/usr/lib/python2.7/site-packages/cstor
/opt/plex/apps/sspl_hl
%defattr(0755,root,root,-)
/usr/bin/cstor
/usr/lib/python2.7/site-packages/cstor/cli/main.py
%defattr(0777,plex,plex,-)
/var/lib/support_bundles



%changelog
* Mon Mar 28 2016 Bhupesh Pant <Bhupesh.Pant@seagate.com>
- Added support_bundle and cluster_node_manager modules in utils .

* Tue Mar 22 2016 Harshada Tupe <harshada.tupe@seagate.com>
- Changed support_bundles directory permissions.

* Thu Feb 18 2016 Bhupesh Pant <bhupesh.pant@seagate.com>
- Changed support_bundle to bundle

* Wed Jan 06 2016 Bhupesh Pant <bhupesh.pant@seagate.com>
- Removed auth.propeties files

* Mon Dec 28 2015 Bhupesh Pant <bhupesh.pant@seagate.com>
- Removed all the unnecessary providers

* Sat Nov 14 2015 Bhupesh Pant <bhupesh.pant@seagate.com>
- Add status provider

* Fri Sep 11 2015 Madhur Nawandar <madhur.nawandar@seagate.com>
- Add support_bundle provider
- Created directory to store support bundles

* Mon Aug 31 2015 Malhar Vora <malhar.vora@seagate.com>
- Add power and fru providers

* Tue Aug 04 2015 Rich Gowman <rich.gowman@seagate.com>
- Add service and node providers
- Add common utils package (used by various providers)

* Thu Jul 23 2015 Rich Gowman <rich.gowman@seagate.com>
- Migrate cstor cli from single script to python module

* Mon Jun 01 2015 David Adair <dadair@seagate.com>
- Add jenkins-friendly template.  Convert to single tarball for all of sspl.

* Thu Apr 23 2015 Rich Gowman <rich.gowman@seagate.com> 0.0.1-1
- Initial RPM
